"""
FYP : 22013

Module:
    Post Benchmark Processor

Description:
    This python script executes the necessary code in the Post Benchmark Processor
    module to compute the Freshness Scores and Query Latencies for the benchmark
    output.

"""

# Imports from built-in modules
import json
import multiprocessing as mp
import sys

# Imports from Post Benchmark Processor Module
from PBP001_freshness_score_processor import Freshness_Score_Processor
from PBP002_query_latency_processor import Query_Latency_Processor

# Calculates Freshness Score for Each Analytical Query
def start_freshness_score_processor(configs):
    processor = Freshness_Score_Processor(output_directory_location = configs["post_benchmark_output_directory"],\
                                          freshness_score_results_directory = configs["freshness_score_results_directory"],\
                                          number_of_analytical_threads = configs["number_of_analytical_threads"],\
                                          number_of_transactional_threads = configs["number_of_transactional_threads"])
    processor.execute()

# Calculates Latency for Each Query Type
def start_query_latency_processor(configs):
    processor = Query_Latency_Processor(output_directory_location = configs["post_benchmark_output_directory"],\
                                        freshness_score_results_directory = configs["freshness_score_results_directory"],\
                                        number_of_analytical_threads = configs["number_of_analytical_threads"],\
                                        number_of_transactional_threads = configs["number_of_transactional_threads"])
    processor.execute()

# Calls the functions defined above to complete post-benchmark processing
def start_post_benchmark_processor(configs):
    fs_process = mp.Process(target=start_freshness_score_processor, args=(configs,))

    ql_process = mp.Process(target=start_query_latency_processor, args=(configs,))

    fs_process.start()
    ql_process.start()
    ql_process.join()
    fs_process.join()



if __name__ == '__main__':

    # Checking if the number of command line arguments is as expected
    if len(sys.argv) == 2:
        # starting the post-benchmark processing
        config_file = sys.argv[1]
        with open(config_file, "r") as f:
            configs = json.loads(f.read())
            start_post_benchmark_processor(configs)

    else:
        print("USAGE:", sys.argv[0], "<JSON config file>")

# End of PBP000_main_processor.py
