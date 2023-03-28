"""
FYP : 22013

Module:
    Post Benchmark Processor - Freshness Score Processor

Description:
    This python script contains the definition of the Freshness_Score_Processor
    class.

    This class computes the Freshness Score for each analytical query and then
    outputs them for each analytical thread in their own files and in a combined
    file where the freshness scores of all analytical queries are outputted with
    the queries sorted in ascending order by their end time.
"""

# Imports from built-in modules
import multiprocessing as mp
import sys

# Imports from Post Benchmark Processor Module
from PBP003_query_data import Freshness_Score_Query_Data_Object
from PBP003_query_data import Query_Statistics_Data_Object

class Freshness_Score_Processor:

    def __init__(self,\
                 output_directory_location,\
                 freshness_score_results_directory,\
                 number_of_analytical_threads,\
                 number_of_transactional_threads\
                 ):

        self.output_directory_location = output_directory_location
        self.freshness_score_results_directory = freshness_score_results_directory
        self.number_of_analytical_threads = number_of_analytical_threads
        self.number_of_transactional_threads = number_of_transactional_threads

        # Stores the transactional query stats for each transactional query
        self.transactional_query_statistics_dictionary = {}

        for i in range(0, number_of_transactional_threads):
            self.transactional_query_statistics_dictionary["thread_"+str(i)] = []

    """
    This function reads the transactional query statistics from all transactional
    query driver thread outputs and stores the stats for each query in the
    statistics dictionary to be used during freshness score calculation.
    """
    def read_transactional_query_statistics(self,):

        # Iterating through each transactional thread output file
        for file_number in range(0, self.number_of_transactional_threads):
            with open(self.freshness_score_results_directory+"Transactional/thread_"+str(file_number)+".csv", "r") as filepointer:
                lines_list = filepointer.readlines()
                number_of_message_lines = 2
                for line in lines_list:

                    # skipping the message lines
                    if number_of_message_lines > 0:
                        number_of_message_lines -= 1
                    else:
                        data_point = line.strip('\n').split('|')

                        # reading the query stats into an object to be stored in the stats dictionary
                        new_query = Query_Statistics_Data_Object(is_transactional = True,
                                                     thread_id = file_number,
                                                     query_id = int(data_point[0]),
                                                     query_type = data_point[1],
                                                     start_time = int(data_point[2]),
                                                     end_time = int(data_point[3])
                                                     )

                        self.transactional_query_statistics_dictionary["thread_"+str(file_number)].append(new_query)

    """
    This function computes the freshness scores for each analytical query for
    its target analytical thread of the analytical query driver and outputs the
    computed freshness scores in the dedicated file for its target analytical
    thread of the analytical query driver.
    """
    def fs_analytical_process_target(self, thread_id, t_q_stat_dictionary, output_directory, fs_result_directory, number_of_transactional_threads):
        output_file = output_directory + "Freshness_Score/thread_" + str(thread_id) + ".csv"

        with open(output_file, "w") as output_fp:
            output_fp.write("Analytical Query ID|Query Type|Start Time|End Time|Freshness Score\n")
            with open(fs_result_directory+"Analytical/thread_"+str(thread_id)+".csv", "r") as fs_input_fp:
                lines_list = fs_input_fp.readlines()
                number_of_message_lines = 2
                for line in lines_list:

                    # skipping the message lines
                    if number_of_message_lines > 0:
                        number_of_message_lines -= 1
                    else:
                        data_point = line.strip('\n').split('|')

                        minimum_transaction_id = int(data_point[4]) + 1

                        # stores the timestamp of the earliest unseen transaction
                        minimum_transaction_timestamp = sys.maxsize
                        if (len(t_q_stat_dictionary["thread_0"]) > minimum_transaction_id):

                            minimum_transaction_timestamp = t_q_stat_dictionary["thread_0"][minimum_transaction_id].end_time

                        for t_thread in range(1, number_of_transactional_threads):

                            # ID of the next unseen transaction for this transactional thread
                            current_transaction_id = int(data_point[t_thread+4]) + 1

                            if (len(t_q_stat_dictionary["thread_"+str(t_thread)]) > current_transaction_id):
                                current_commit_timestamp = t_q_stat_dictionary["thread_"+str(t_thread)][current_transaction_id].end_time
                                if(minimum_transaction_timestamp > current_commit_timestamp):
                                    minimum_transaction_timestamp = current_commit_timestamp

                        # Freshness Score of the current query
                        freshness_score = int(data_point[2]) - minimum_transaction_timestamp

                        if minimum_transaction_timestamp == sys.maxsize:
                            freshness_score = 0

                        output_line = data_point[0] + "|" + data_point[1] + "|" + data_point[2] + "|" + data_point[3] + "|" + str(freshness_score) + "\n"
                        output_fp.write(output_line)






    # Executes the Freshness Score Processor object
    def execute(self,):

        # populate dictionary
        self.read_transactional_query_statistics()

        print("FSP: READING TRANSACTIONAL QUERY STATISTICS")

        # stores processes spawned for each analytical thread
        analytical_thread_process_list = []

        # initializing one process per analytical thread for freshness score calculation
        for i in range(0, self.number_of_analytical_threads):
            temp_process = mp.Process(target=self.fs_analytical_process_target, args=(i, self.transactional_query_statistics_dictionary, self.output_directory_location, self.freshness_score_results_directory, self.number_of_transactional_threads))
            analytical_thread_process_list.append(temp_process)
            temp_process.start()

        # joining all spawned processes
        for process in analytical_thread_process_list:
            process.join()

        """
        After the freshness scores for each analytical thread of the analytical
        query driver have been generated individually, we combine the output in
        a single file.
        """
        combined_output_file = self.output_directory_location + "Freshness_Score/combined_freshness_scores.csv"
        with open(combined_output_file, "w") as output_fp:
            output_fp.write("Combined Freshness Scores sorted by end time\n")
            output_fp.write("Query Type|Start Time|End Time|Freshness Score\n")

            freshness_score_output_list = []

            for thread_id in range(0, self.number_of_analytical_threads):
                fs_file_name = self.output_directory_location + "Freshness_Score/thread_" + str(thread_id) + ".csv"
                with open(fs_file_name, "r") as input_fs_file:
                    lines_list = input_fs_file.readlines()
                    number_of_message_lines = 1
                    for line in lines_list:
                        if number_of_message_lines > 0:
                            number_of_message_lines -= 1
                        else:
                            data_point = line.strip('\n').split('|')

                            new_query = Freshness_Score_Query_Data_Object(query_type = data_point[1],
                                                         start_time = int(data_point[2]),
                                                         end_time = int(data_point[3]),
                                                         freshness_score = int(data_point[4]),
                                                         )
                            freshness_score_output_list.append(new_query)

            freshness_score_output_list.sort(key=lambda x: x.end_time)

            for query_stat in freshness_score_output_list:
                output_line = query_stat.query_type + "|" + str(query_stat.start_time) + "|" + str(query_stat.end_time) + "|" + str(query_stat.freshness_score) + "\n"
                output_fp.write(output_line)

        print("FSP: FRESHNESS SCORES COMPUTED")
