#!/usr/bin/env python3
"""
Serverless Data Analysis with Dataflow: Side Inputs (Python)

This script demonstrates how to build a serverless data analysis pipeline using
Apache Beam and Google Cloud Dataflow. The pipeline performs the following tasks:

    1. Reads Java file contents from a BigQuery table.
       - The BigQuery table (`cloud-training-demos.github_repos.contents_java`)
         contains a field called 'content' with Java file data.
    
    2. Splits each file’s content into individual lines.
       - A custom PTransform (ToLines) is used to extract the lines from the
         BigQuery records.
    
    3. Filters the lines to identify “help” calls.
       - Help calls are identified as lines containing the words "FIXME" or "TODO".
       - For example, if a file has 3 lines with "FIXME" and 2 lines with "TODO",
         it is associated with 5 calls for help.
    
    4. Counts the total number of help calls across all files.
    
    5. Uses a side input (a dictionary of popular packages with scores) to compute
       a final score. The side input is used at the “Scores” step of the pipeline.
       - In Python, the side input is converted into a dictionary using AsDict.
    
    6. Writes the computed result to a Cloud Storage bucket (in the "javahelp" folder).

Execution:
    To run the pipeline locally using the DirectRunner:
        python3 JavaProjectsThatNeedHelp.py --bucket YOUR_BUCKET \
                                             --project YOUR_PROJECT \
                                             --region YOUR_REGION \
                                             --DirectRunner

    To run the pipeline on the cloud using the DataFlowRunner:
        python3 JavaProjectsThatNeedHelp.py --bucket YOUR_BUCKET \
                                             --project YOUR_PROJECT \
                                             --region YOUR_REGION \
                                             --DataFlowRunner

Make sure you have set up your Cloud project, bucket, and have enabled the
Dataflow and BigQuery APIs as described in the lab instructions.

Author: Your Name
Date: YYYY-MM-DD
"""

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# -----------------------------------------------------------------------------
# Custom PTransform
# -----------------------------------------------------------------------------
class ToLines(beam.PTransform):
    """
    A custom PTransform that converts each BigQuery record into individual lines.
    
    Each record is expected to be a dictionary with a 'content' field (string).
    This transform splits the 'content' into lines using the newline character.
    """
    def expand(self, pcoll):
        return pcoll | beam.FlatMap(lambda record: record.get('content', '').splitlines())


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def compute_score(help_count, popular_packages):
    """
    Compute a final score by combining the total help count with a side input
    that contains package popularity scores.
    
    The scoring is computed as:
        score = help_count * (average popularity score)
    
    Args:
        help_count (int): The total number of help calls across all files.
        popular_packages (dict): A dictionary mapping package names to their scores.
        
    Returns:
        float: The computed score.
    """
    if popular_packages:
        avg_popularity = sum(popular_packages.values()) / len(popular_packages)
    else:
        avg_popularity = 1
    return help_count * avg_popularity


# -----------------------------------------------------------------------------
# Main Pipeline Definition
# -----------------------------------------------------------------------------
def run(argv=None):
    """
    Main entry point; defines and runs the Apache Beam pipeline.
    
    Command-line arguments:
        --bucket          : Name of the Cloud Storage bucket to use.
        --project         : Your Google Cloud project ID.
        --region          : GCP region for running the pipeline.
        --DirectRunner    : Flag to use the DirectRunner (local execution).
        --DataFlowRunner  : Flag to use the DataFlowRunner (cloud execution).
    
    The pipeline performs the following steps:
        1. Reads Java file content from BigQuery.
        2. Splits each record into lines (using the ToLines transform).
        3. Filters for lines containing "FIXME" or "TODO".
        4. Counts the total number of help calls.
        5. Uses a side input (popular package scores) to compute a final score.
        6. Writes the result to Cloud Storage.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', required=True, help='Cloud Storage bucket name')
    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--region', required=True, help='GCP region')
    # Flags for selecting the runner (local vs. cloud)
    parser.add_argument('--DirectRunner', action='store_true',
                        help='Use DirectRunner for local execution')
    parser.add_argument('--DataFlowRunner', action='store_true',
                        help='Use DataFlowRunner for cloud execution')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Determine which runner to use; default to DirectRunner if no flag is provided.
    if known_args.DataFlowRunner:
        runner = 'DataFlowRunner'
    else:
        runner = 'DirectRunner'
    logging.info('Using runner: %s', runner)

    # Add the runner to pipeline arguments.
    pipeline_args.extend(['--runner', runner])

    # Set up pipeline options with project, region, and temp_location.
    pipeline_options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        temp_location='gs://{}/temp'.format(known_args.bucket)
    )
    # Ensure that the main session is saved for pickling dependencies.
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # -----------------------------------------------------------------------------
    # Step 1: Read Data from BigQuery
    # -----------------------------------------------------------------------------
    # Define the SQL query that retrieves the Java file content.
    query = """
        SELECT content
        FROM `cloud-training-demos.github_repos.contents_java`
        LIMIT 1000
    """

    with beam.Pipeline(options=pipeline_options) as p:
        bq_data = (
            p 
            | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
                query=query, 
                use_standard_sql=True)
        )

        # -----------------------------------------------------------------------------
        # Step 2: Process the Data
        # -----------------------------------------------------------------------------
        # a) Convert each record into individual lines.
        lines = bq_data | 'ToLines' >> ToLines()

        # b) Filter for lines that include a call for help.
        #    Help requests are identified by the presence of "FIXME" or "TODO".
        help_lines = lines | 'FilterHelpLines' >> beam.Filter(
            lambda line: ('FIXME' in line) or ('TODO' in line)
        )

        # c) Count the total number of help calls across all files.
        total_help_calls = help_lines | 'CountHelpCalls' >> beam.combiners.Count.Globally()

        # -----------------------------------------------------------------------------
        # Step 3: Use a Side Input to Compute a Score
        # -----------------------------------------------------------------------------
        # Create a PCollection of popular packages and their scores.
        popular_packages = (
            p 
            | 'CreatePopularPackages' >> beam.Create([
                ('com.google.devtools.build', 10),
                ('org.apache.beam', 5),
                # You can add more package-score pairs as needed.
            ])
        )
        # Convert the popular_packages PCollection into a dictionary for use as a side input.
        popular_packages_dict = popular_packages | 'PopularPackagesAsDict' >> beam.transforms.combiners.ToDict()

        # Compute a final score by combining the help call count with the side input.
        scored_result = (
            total_help_calls 
            | 'ComputeScore' >> beam.Map(
                lambda count, pop: compute_score(count, pop),
                pop=beam.pvalue.AsDict(popular_packages)
            )
        )

        # -----------------------------------------------------------------------------
        # Step 4: Write the Result to Cloud Storage
        # -----------------------------------------------------------------------------
        # The result is written as a text file in the "javahelp" folder within the bucket.
        output_path = 'gs://{}/javahelp/result'.format(known_args.bucket)
        scored_result | 'WriteOutput' >> beam.io.WriteToText(output_path)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
