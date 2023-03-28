# COMP4801_FYP_EVALUATION_Post_Benchmark_Processor

## FYP: 22013

### FYP Team

**Student:** SRIVASTAVA Dhruv (3035667792)

**Supervisor:** Dr. Heming Cui

## Description

Post Benchmark Processor for HTAP Graph Database Benchmark.

## Usage Instructions

Run the Post Benchmark Processor after executing the benchmark with freshness-
score mode enabled.

Fill in the configuration details in ```build_config.py``` and execute it to
generate a configuration file for the Post-Benchmark Processor. Finally, execute
the processor by running:

```python PBP000_main_processor.py <JSON configuration file>```

The output will appear in the target output directory as two folders:

- ```Freshness_Score``` containing freshness score of each analytical query per
analytical query driver thread in a dedicated file named ```thread_<x>.csv```,
with ```<x>``` following the thread ID of the analytical thread. The folder will
also contain a ```combined_freshness_scores.csv``` file containing freshness
score of each analytical query in every analytical query driver thread, sorted
in ascending order of the ```end_time``` of each analytical query.

- ```Query_Latency``` containing the latencies of each query sent out by the
hybrid query driver for each query type put in a dedicated file for each query
type named after the query type ```TQ_*.csv```/```AQ_*.csv```. The queries are
sorted in ascending order of the ```end_time``` in each file.

## Module Components Description

| File Name | Description |
|-----------|-------------|
|PBP000_main_processor.py| Script to execute all post-benchmark processors |
|PBP001_freshness_score_processor.py| Contains the Freshness Score Processor |
|PBP002_query_latency_processor.py| Contains the Query Latency Processor |
|PBP003_query_data.py| Contains class definitions to encapsulate query statistics for processing in the 2 processors |
|build_config.py| Helper script to generate the configuration file for the post benchmark processor|
