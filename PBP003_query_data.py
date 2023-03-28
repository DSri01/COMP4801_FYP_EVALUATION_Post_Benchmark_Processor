"""
FYP : 22013

Module:
    Post Benchmark Processor

Description:
    This python script contains the definition of the Query_Statistics_Data_Object
    and the Freshness_Score_Query_Data_Object classes.

    These classes supplement the two post-benchmark processors to compute and
    output their results by encapsulting details of a query to be stored in a
    list/dictionary.
"""

class Query_Statistics_Data_Object:
    """
    Description:
        Used in both Freshness_Score_Processor and Query_Latency_Processor classes
        to encapsulate the statistics of transactional/analytical queries.
    """

    def __init__(self, is_transactional, thread_id, query_id, query_type, start_time, end_time):
        self.is_transactional = is_transactional
        self.thread_id = thread_id
        self.query_id = query_id
        self.query_type = query_type
        self.start_time = start_time
        self.end_time = end_time

        self.latency = end_time - start_time

class Freshness_Score_Query_Data_Object:
    """
    Description:
        Used in the Freshness_Score_Processor class to encapsulate the statistics
        of analytical queries along with their computed freshness score.
    """

    def __init__(self, query_type, start_time, end_time, freshness_score):
        self.query_type = query_type
        self.start_time = start_time
        self.end_time = end_time
        self.freshness_score = freshness_score
