"""
FYP : 22013

Module:
    Post Benchmark Processor - Query Latency Processor

Description:
    This python script contains the definition of the Query_Latency_Processor
    class.

    This class computes the latency for each query of each query type and then
    outputs the latency according to the query type in a file sorted in
    ascending order by the query end time.
"""

# Imports from built-in modules
import multiprocessing as mp

# Imports from Post Benchmark Processor Module
from PBP003_query_data import Query_Statistics_Data_Object

class Query_Latency_Processor:

    def __init__(self,
                 output_directory_location,\
                 freshness_score_results_directory,\
                 number_of_analytical_threads,\
                 number_of_transactional_threads):

        self.output_directory_location = output_directory_location
        self.freshness_score_results_directory = freshness_score_results_directory
        self.number_of_analytical_threads = number_of_analytical_threads
        self.number_of_transactional_threads = number_of_transactional_threads

        self.transactional_query_types = ["TQ_1_A", "TQ_1_B", "TQ_2", "TQ_3", "TQ_4", "TQ_5", "TQ_6", "TQ_7", "TQ_8"]

        self.analytical_query_types = ["AQ_1", "AQ_2", "AQ_3", "AQ_4", "AQ_5", "AQ_6"]

    """
    This function reads the output of the transactional query driver to find the
    query latency of its target transactional query type and output the latencies
    in a dedicated file for the transactional query type.
    """
    def ql_transactional_process_target(self, target_query_type):

        transactional_query_stat_list = []

        for t_thread in range(0, self.number_of_transactional_threads):
            with open(self.freshness_score_results_directory+"Transactional/thread_"+str(t_thread)+".csv", "r") as filepointer:
                lines_list = filepointer.readlines()
                number_of_message_lines = 2
                for line in lines_list:
                    if number_of_message_lines > 0:
                        number_of_message_lines -= 1
                    else:
                        data_point = line.strip('\n').split('|')

                        new_query = Query_Statistics_Data_Object(is_transactional = True,
                                                     thread_id = t_thread,
                                                     query_id = int(data_point[0]),
                                                     query_type = data_point[1],
                                                     start_time = int(data_point[2]),
                                                     end_time = int(data_point[3])
                                                     )
                        if (new_query.query_type == target_query_type):
                            transactional_query_stat_list.append(new_query)
        transactional_query_stat_list.sort(key=lambda x: x.end_time)
        with open(self.output_directory_location + "Query_Latency/" + target_query_type, "w") as output_fp:
            output_fp.write("Observed Latency for "+ target_query_type+ " sorted by commit (end) time\n")
            output_fp.write("Start Time|End Time|Observed Latency\n")
            for query in transactional_query_stat_list:
                output_line = str(query.start_time) + "|" + str(query.end_time) + "|" + str(query.latency) + "\n"
                output_fp.write(output_line)


    """
    This function reads the output of the analytical query driver to find the
    query latency of its target analytical query type and output the latencies
    in a dedicated file for the analytical query type.
    """
    def ql_analytical_process_target(self, target_query_type):
        analytical_query_stat_list = []

        for t_thread in range(0, self.number_of_analytical_threads):
            with open(self.freshness_score_results_directory+"Analytical/thread_"+str(t_thread)+".csv", "r") as filepointer:
                lines_list = filepointer.readlines()
                number_of_message_lines = 2
                for line in lines_list:
                    if number_of_message_lines > 0:
                        number_of_message_lines -= 1
                    else:
                        data_point = line.strip('\n').split('|')

                        new_query = Query_Statistics_Data_Object(is_transactional = False,
                                                     thread_id = t_thread,
                                                     query_id = int(data_point[0]),
                                                     query_type = data_point[1],
                                                     start_time = int(data_point[2]),
                                                     end_time = int(data_point[3])
                                                     )
                        if (new_query.query_type == target_query_type):
                            analytical_query_stat_list.append(new_query)
        analytical_query_stat_list.sort(key=lambda x: x.end_time)
        with open(self.output_directory_location + "Query_Latency/" + target_query_type, "w") as output_fp:
            output_fp.write("Observed Latency for "+ target_query_type+ " sorted by end time\n")
            output_fp.write("Start Time|End Time|Observed Latency\n")
            for query in analytical_query_stat_list:
                output_line = str(query.start_time) + "|" + str(query.end_time) + "|" + str(query.latency) + "\n"
                output_fp.write(output_line)

    # Executes the Query Latency Processor object
    def execute(self,):

        # stores the process spawned for each query type
        qd_thread_process_list = []

        for i in range(0, 15):
            if (i < 9):
                temp_process = mp.Process(target=self.ql_transactional_process_target, args=(self.transactional_query_types[i],))
                qd_thread_process_list.append(temp_process)
                temp_process.start()
            else:
                temp_process = mp.Process(target=self.ql_analytical_process_target, args=(self.analytical_query_types[i-9],))
                qd_thread_process_list.append(temp_process)
                temp_process.start()

        # joining all spawned processes
        for process in qd_thread_process_list:
            process.join()

        print("QLP: QUERY LATENCY COMPUTED")
