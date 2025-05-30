# Streaming Data Processing: Streaming Data Pipelines

## Overview

In this lab, you will use Dataflow to collect traffic events from simulated traffic sensor data made available through Google Cloud Pub/Sub, process them into an actionable average, and store the raw data in BigQuery for later analysis.

> **Note:** Streaming pipelines are currently supported in Java only.

## Objectives

- Launch a Dataflow job
- Understand data flow in a pipeline
- Connect Pub/Sub and BigQuery
- Monitor autoscaling
- Explore logging and metrics
- Create alerts and dashboards with Cloud Monitoring

## Setup

1. Sign into Qwiklabs and start the lab
2. Use provided credentials to log into the Google Cloud Console
3. Validate IAM permissions for compute service account to have Editor role

## Task List

### Task 1. Preparation

- SSH into `training-vm` via Google Cloud Console
- Verify setup with `ls /training`
- Clone repository: `git clone https://github.com/GoogleCloudPlatform/training-data-analyst`
- Set env variables: `source /training/project_env.sh`

### Task 2. Create BigQuery Dataset and Cloud Storage Bucket

- Create BigQuery dataset: `demos`
- Verify default GCS bucket: name = `PROJECT_ID`, storage class = `Regional`, location = `Lab GCP Region`

### Task 3. Simulate Traffic Sensor Data into Pub/Sub

- Run simulator: `/training/sensor_magic.sh`
- Open second SSH terminal
- Reinitialize environment: `source /training/project_env.sh`

### Task 4. Launch Dataflow Pipeline

- Ensure API is enabled:
  ```bash
  gcloud services disable dataflow.googleapis.com --force
  gcloud services enable dataflow.googleapis.com
  ```
- Navigate to directory: `cd ~/training-data-analyst/courses/streaming/process/sandiego`
- Run pipeline:
  ```bash
  export REGION=Lab GCP Region
  ./run_oncloud.sh $DEVSHELL_PROJECT_ID $BUCKET AverageSpeeds
  ```

### Task 5. Explore the Pipeline

- Navigate to Console > Dataflow > Your Job
- Open and review the job steps vs. code (e.g., `AverageSpeeds.java`)
- Locate steps: `GetMessages`, `Time Window`, `BySensor`, `AvgBySensor`, `ToBQRow`, `BigQueryIO.Write`

### Task 6. Determine Throughput Rates

- In Console > Dataflow > Your Job
- Select `GetMessages` to view metrics: System Lag, Elements Added
- Compare with `Time Window` step

### Task 7. Review BigQuery Output

- Query outputs:
  ```sql
  SELECT * FROM `demos.average_speeds` ORDER BY timestamp DESC LIMIT 100;
  SELECT MAX(timestamp) FROM `demos.average_speeds`;
  SELECT * FROM `demos.average_speeds`
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 10 MINUTE)
  ORDER BY timestamp DESC LIMIT 100;
  ```

### Task 8. Observe Autoscaling

- In Dataflow Console > Your Job
- Review Job metrics > Autoscaling
- View history of worker count

### Task 9. Refresh Sensor Data Simulation

- Stop simulator with `CTRL+C`, restart:
  ```bash
  cd ~/training-data-analyst/courses/streaming/publish
  ./send_sensor_data.py --speedFactor=60 --project $DEVSHELL_PROJECT_ID
  ```

### Task 10. Cloud Monitoring Integration

- Metrics include job status, system lag, elapsed time, vCPU usage, byte count
- User-defined metrics (SDK Aggregators) reported every 30s

### Task 11. Explore Metrics

- Go to Console > Monitoring > Metrics Explorer
- Add metrics: `Dataflow Job > Job > Data watermark lag`, then `System lag`

### Task 12. Create Alerts

- Console > Monitoring > Alerting > Create Policy
- Add metric: `Dataflow Job > Job > System Lag`
- Threshold: above 5 sec for 1 min
- Notification channel: email to lab account
- Name: `MyAlertPolicy`

### Task 13. Setup Dashboards

- Console > Monitoring > Dashboards > Create Dashboard
- Name: `My Dashboard`
- Add Line Chart: `Dataflow Job > Job > System Lag`
- Filter by `project_id`

## End Lab

- Click "End Lab" when complete
- Optionally provide feedback and rating

---

**Copyright © 2022 Google**
