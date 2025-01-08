# Google Cloud SQL: Loading Taxi Data
# Lab Objective: Import NYC taxi data into Cloud SQL and perform basic data analysis.

# -------------------------------
# Task 1: Preparing the Environment
# -------------------------------

# Set up environment variables for project ID and storage bucket
export PROJECT_ID=$(gcloud info --format='value(config.project)')
export BUCKET=${PROJECT_ID}-ml

# -------------------------------
# Task 2: Create Cloud SQL Instance
# -------------------------------

# Create a Cloud SQL instance named 'taxi'
gcloud sql instances create taxi \
    --tier=db-n1-standard-1 --activation-policy=ALWAYS

# Set a root password for the SQL instance
gcloud sql users set-password root --host % --instance taxi \
    --password Passw0rd

# Whitelist the current Cloud Shell's IP for SQL instance management
export ADDRESS=$(wget -qO - http://ipecho.net/plain)/32
gcloud sql instances patch taxi --authorized-networks $ADDRESS

# Get the IP address of the SQL instance
MYSQLIP=$(gcloud sql instances describe \
taxi --format="value(ipAddresses.ipAddress)")
echo $MYSQLIP

# -------------------------------
# Task 3: Create Database and Table
# -------------------------------

# Log in to the MySQL CLI
mysql --host=$MYSQLIP --user=root \
      --password --verbose

# Run the following SQL commands to create the database and table schema:
# ---------------------------------------
# Create database and trips table schema
create database if not exists bts;
use bts;

drop table if exists trips;

create table trips (
  vendor_id VARCHAR(16),		
  pickup_datetime DATETIME,
  dropoff_datetime DATETIME,
  passenger_count INT,
  trip_distance FLOAT,
  rate_code VARCHAR(16),
  store_and_fwd_flag VARCHAR(16),
  payment_type VARCHAR(16),
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  imp_surcharge FLOAT,
  total_amount FLOAT,
  pickup_location_id VARCHAR(16),
  dropoff_location_id VARCHAR(16)
);
# ---------------------------------------

# Verify the table creation
describe trips;

# Exit MySQL CLI
exit

# -------------------------------
# Task 4: Add Data to Cloud SQL Instance
# -------------------------------

# Download CSV files from Cloud Storage
gcloud storage cp gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_1.csv trips.csv-1
gcloud storage cp gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_2.csv trips.csv-2

# Connect to the MySQL CLI with local-infile enabled
mysql --host=$MYSQLIP --user=root --password --local-infile

# Load data from CSV files into the trips table
# ---------------------------------------
use bts;

LOAD DATA LOCAL INFILE 'trips.csv-1' INTO TABLE trips
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,
 rate_code,store_and_fwd_flag,payment_type,fare_amount,extra,
 mta_tax,tip_amount,tolls_amount,imp_surcharge,total_amount,
 pickup_location_id,dropoff_location_id);

LOAD DATA LOCAL INFILE 'trips.csv-2' INTO TABLE trips
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,
 rate_code,store_and_fwd_flag,payment_type,fare_amount,extra,
 mta_tax,tip_amount,tolls_amount,imp_surcharge,total_amount,
 pickup_location_id,dropoff_location_id);
# ---------------------------------------

# Exit MySQL CLI
exit

# -------------------------------
# Task 5: Data Integrity Checks
# -------------------------------

# Check for distinct pickup locations
mysql --host=$MYSQLIP --user=root --password --execute="use bts; select distinct(pickup_location_id) from trips;"

# Query max and min trip distances
mysql --host=$MYSQLIP --user=root --password --execute="use bts; select max(trip_distance), min(trip_distance) from trips;"

# Count trips with a trip distance of 0
mysql --host=$MYSQLIP --user=root --password --execute="use bts; select count(*) from trips where trip_distance = 0;"

# Count trips with a negative fare amount
mysql --host=$MYSQLIP --user=root --password --execute="use bts; select count(*) from trips where fare_amount < 0;"

# Group by payment types
mysql --host=$MYSQLIP --user=root --password --execute="use bts; select payment_type, count(*) from trips group by payment_type;"

# -------------------------------
# End of Lab
# -------------------------------
