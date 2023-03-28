"""
FYP : 22013

Module:
    Post Benchmark Processor

Description:
    This python script is a supporting script to generate the JSON configuration
    file for the Post Benchmark Processor.
"""

import json
import sys

configs = {}

configs["number_of_analytical_threads"] = 5
configs["number_of_transactional_threads"] = 5

# Location of the freshness score results directory of the Hybrid Query Driver
configs["freshness_score_results_directory"] = ""

configs["post_benchmark_output_directory"] = "Post_Benchmark_Processor_Output/"

configs

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage:", sys.argv[0],"<destination JSON file name>")
    else:
        with open(sys.argv[1], 'w') as file:
            file.write(json.dumps(configs))
            file.close()
