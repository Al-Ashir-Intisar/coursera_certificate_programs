# Running Apache Spark Jobs on Cloud Dataproc

This document contains step-by-step instructions for migrating and running Apache Spark jobs on Google Cloud Dataproc. These instructions are annotated for clarity and ready to be pushed to your GitHub repository.

---

## **Lab Overview**

- Learn how to migrate Apache Spark code to Cloud Dataproc.
- Progressively move from a "Lift and Shift" approach to cloud-native and cloud-optimized solutions.

---

## **Setup**

### **1. Create a Dataproc Cluster**

```bash
# In Google Cloud Console:
# 1. Navigate to Dataproc > Create Cluster
# 2. Set Cluster Name: sparktodp
# 3. Region: <REGION>
# 4. Version: 2.1 (Debian 11, Hadoop 3.3, Spark 3.3)
# 5. Machine Type (Manager/Worker): e2-standard-2
# 6. Disk Size: 30 GB
# 7. Enable Jupyter Notebook via Component Gateway
```

---

## **Task 1: Lift and Shift**

### **Clone the Source Repository**

```bash
# Clone the lab's GitHub repository:
git -C ~ clone https://github.com/GoogleCloudPlatform/training-data-analyst
```

### **Configure Dataproc Storage**

```bash
# Export the Dataproc storage bucket:
export DP_STORAGE="gs://$(gcloud dataproc clusters describe sparktodp --region=<REGION> --format=json | jq -r '.config.configBucket')"
```

### **Copy Sample Notebooks**

```bash
# Copy Jupyter notebooks to Dataproc's default Cloud Storage bucket:
gcloud storage cp ~/training-data-analyst/quests/sparktobq/*.ipynb $DP_STORAGE/notebooks/jupyter
```

---

## **Task 2: Decouple Storage from Compute**

### **Move Data to Cloud Storage**

```bash
# Create a Cloud Storage bucket:
export PROJECT_ID=$(gcloud info --format='value(config.project)')
gcloud storage buckets create gs://$PROJECT_ID

# Copy data to the Cloud Storage bucket:
wget https://storage.googleapis.com/cloud-training/dataengineering/lab_assets/sparklab/kddcup.data_10_percent.gz
gcloud storage cp kddcup.data_10_percent.gz gs://$PROJECT_ID/
```

### **Modify Jupyter Notebook**

- Replace `hdfs://` references with `gs://<YOUR_BUCKET_NAME>` in the Spark job code:

```python
data_file = "gs://<YOUR_BUCKET_NAME>/kddcup.data_10_percent.gz"
```

---

## **Task 3: Automate with PySpark**

### **Create a Standalone Python Script**

In Jupyter, add `%%writefile` commands to save code into a `.py` file:

```python
%%writefile spark_analysis.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("kdd").getOrCreate()
data_file = "gs://{}/kddcup.data_10_percent.gz".format(BUCKET)
raw_rdd = sc.textFile(data_file).cache()
# Continue with transformations...
```

### **Save and Export Outputs**

```python
# Save output to Cloud Storage:
connections_by_protocol.write.format("csv").mode("overwrite").save(
    "gs://{}/sparktodp/connections_by_protocol".format(BUCKET))
```

---

## **Task 4: Submit Dataproc Jobs**

### **Run the Python Script Locally**

```bash
# Run the standalone Python script:
BUCKET=$(gcloud info --format='value(config.project)')
/opt/conda/miniconda3/bin/python spark_analysis.py --bucket=$BUCKET
```

### **Submit the Spark Job to Dataproc**

```bash
# Create a job submission script:
nano submit_onejob.sh
```

#### **Job Submission Script Content**

```bash
#!/bin/bash
gcloud dataproc jobs submit pyspark \
       --cluster sparktodp \
       --region <REGION> \
       spark_analysis.py \
       -- --bucket=$1
```

```bash
# Make the script executable:
chmod +x submit_onejob.sh

# Run the job:
./submit_onejob.sh $PROJECT_ID
```

---

## **Clean Up**

### **Delete Dataproc Cluster**

```bash
gcloud dataproc clusters delete sparktodp --region=<REGION>
```

---

## **Additional Notes**

- Always ensure your Cloud Storage bucket is properly referenced.
- Use `%%writefile` and `%%writefile -a` in Jupyter to create and append to standalone Python scripts.
- For output visualizations, replace `%matplotlib inline` with `matplotlib.use('agg')` for standalone compatibility.

---

### **References**

- [Google Cloud Dataproc Documentation](https://cloud.google.com/dataproc/docs)
- [Lab Source Repository](https://github.com/GoogleCloudPlatform/training-data-analyst)

---
