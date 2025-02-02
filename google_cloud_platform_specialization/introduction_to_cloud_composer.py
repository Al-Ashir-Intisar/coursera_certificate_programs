#!/usr/bin/env python3
"""
An Introduction to Cloud Composer 2.5

This DAG demonstrates a simple workflow that:
  1. Creates a Cloud Dataproc cluster.
  2. Runs the Hadoop wordcount example on the cluster.
  3. Deletes the Dataproc cluster after the job completes.

The workflow uses Airflow variables to define key parameters:
  - gcp_project: Your Google Cloud project ID.
  - gce_zone: The Compute Engine zone where the Dataproc cluster will be created.
  - gce_region: The Compute Engine region where the Dataproc cluster will be created.
  - gcs_bucket: A Cloud Storage bucket to store the Hadoop job output.

Setup and Execution:
  - Upload this DAG file to the DAGs folder of your Cloud Composer environment.
  - Cloud Composer will parse the DAG and schedule it automatically.
  - The DAG is scheduled to run once per day. The start_date is set to yesterday so that
    the workflow is triggered immediately.
  - Monitor the progress in the Airflow web UI and view output in your designated Cloud Storage bucket.

Additional requirements:
  - Ensure that your project has the required APIs enabled (Composer, Dataproc, Cloud Storage).
  - Ensure proper IAM roles have been granted to the Cloud Composer service account.

Author: Your Name
Date: YYYY-MM-DD
"""

import datetime
import os

# Import Airflow models and operators.
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Define the output path for the Hadoop wordcount job.
# The output file is created inside the bucket specified by the Airflow variable 'gcs_bucket'
# and includes a timestamp in its path.
output_file = os.path.join(
    models.Variable.get('gcs_bucket'),
    'wordcount',
    datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
) + os.sep

# Path to the Hadoop wordcount example JAR available on every Cloud Dataproc cluster.
WORDCOUNT_JAR = 'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'

# Arguments to pass to the Hadoop wordcount job.
# This example uses a public text file (Shakespeare’s text) as input.
wordcount_args = ['wordcount', 'gs://pub/shakespeare/rose.txt', output_file]

# -----------------------------------------------------------------------------
# DAG Default Arguments
# -----------------------------------------------------------------------------

# Set the start date to yesterday so that the DAG is scheduled to run immediately.
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time()
)

default_dag_args = {
    'start_date': yesterday,
    # Disable email notifications on failure or retry.
    'email_on_failure': False,
    'email_on_retry': False,
    # Retry once after a 5-minute delay if a task fails.
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # Retrieve the project ID from the Airflow variable 'gcp_project'.
    'project_id': models.Variable.get('gcp_project')
}

# -----------------------------------------------------------------------------
# DAG Definition
# -----------------------------------------------------------------------------

with models.DAG(
    'composer_hadoop_tutorial',                     # Unique name for the DAG.
    schedule_interval=datetime.timedelta(days=1),    # The DAG runs once per day.
    default_args=default_dag_args                    # Default arguments for all tasks.
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Create Cloud Dataproc Cluster
    # -------------------------------------------------------------------------
    # This task creates a Cloud Dataproc cluster with a unique name that includes the date.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        num_workers=2,
        region=models.Variable.get('gce_region'),
        zone=models.Variable.get('gce_zone'),
        image_version='2.0',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2'
    )

    # -------------------------------------------------------------------------
    # Task 2: Run Hadoop Wordcount Job
    # -------------------------------------------------------------------------
    # This task runs a Hadoop job on the created Dataproc cluster using the pre-installed example JAR.
    run_dataproc_hadoop = dataproc_operator.DataProcHadoopOperator(
        task_id='run_dataproc_hadoop',
        region=models.Variable.get('gce_region'),
        main_jar=WORDCOUNT_JAR,
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        arguments=wordcount_args
    )

    # -------------------------------------------------------------------------
    # Task 3: Delete Cloud Dataproc Cluster
    # -------------------------------------------------------------------------
    # This task deletes the Cloud Dataproc cluster to avoid ongoing costs.
    # The trigger_rule ALL_DONE ensures that the cluster is deleted even if the job fails.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        region=models.Variable.get('gce_region'),
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

    # -----------------------------------------------------------------------------
    # Task Dependencies
    # -----------------------------------------------------------------------------
    # The tasks are executed sequentially:
    #   1. Create Dataproc cluster.
    #   2. Run the Hadoop wordcount job.
    #   3. Delete the Dataproc cluster.
    create_dataproc_cluster >> run_dataproc_hadoop >> delete_dataproc_cluster
