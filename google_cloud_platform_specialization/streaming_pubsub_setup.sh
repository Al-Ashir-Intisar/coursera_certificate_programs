#!/bin/bash

# Streaming Data Processing: Publish Streaming Data into Pub/Sub
# Author: [Your Name]
# Description: This script sets up Google Cloud Pub/Sub for streaming data processing.

# ===============================================
# Task 1: Preparation
# ===============================================

echo "Setting up environment variables..."
export DEVSHELL_PROJECT_ID=$(gcloud config get-value project)

# Clone the Google Cloud training repository (if not already cloned)
echo "Cloning training data repository..."
git clone https://github.com/GoogleCloudPlatform/training-data-analyst || echo "Repository already cloned."

# Navigate to the lab directory
cd ~/training-data-analyst/courses/streaming/publish || exit

# ===============================================
# Task 2: Create Pub/Sub Topic and Subscription
# ===============================================

# Create a Pub/Sub topic
echo "Creating Pub/Sub topic: sandiego..."
gcloud pubsub topics create sandiego

# Publish a simple message to the topic
echo "Publishing test message to topic..."
gcloud pubsub topics publish sandiego --message "hello"

# Create a subscription for the topic
echo "Creating subscription: mySub1..."
gcloud pubsub subscriptions create --topic sandiego mySub1

# Pull the first message from the subscription
echo "Pulling messages from subscription..."
gcloud pubsub subscriptions pull --auto-ack mySub1

# Publish another message and pull it again
echo "Publishing another message and verifying reception..."
gcloud pubsub topics publish sandiego --message "hello again"
gcloud pubsub subscriptions pull --auto-ack mySub1

# Delete the subscription
echo "Deleting subscription mySub1..."
gcloud pubsub subscriptions delete mySub1

# ===============================================
# Task 3: Simulate Traffic Sensor Data into Pub/Sub
# ===============================================

# Navigate to the correct directory
cd ~/training-data-analyst/courses/streaming/publish || exit

# Download the sensor data dataset
echo "Downloading traffic sensor dataset..."
./download_data.sh

# Simulate streaming sensor data (running continuously)
echo "Starting sensor data simulator..."
./send_sensor_data.py --speedFactor=60 --project $DEVSHELL_PROJECT_ID &

# ===============================================
# Task 4: Verify That Messages Are Received
# ===============================================

# Open a second terminal (manual step required)
echo "Open a second SSH terminal manually and run the following commands:"

echo "Navigate to the lab directory:"
echo "cd ~/training-data-analyst/courses/streaming/publish"

echo "Create a new subscription and pull messages:"
echo "gcloud pubsub subscriptions create --topic sandiego mySub2"
echo "gcloud pubsub subscriptions pull --auto-ack mySub2"

echo "After verifying the messages, delete the subscription:"
echo "gcloud pubsub subscriptions delete mySub2"

# ===============================================
# Task 5: Cleanup
# ===============================================

# Stop the sensor simulator
echo "Stopping sensor data simulator..."
pkill -f send_sensor_data.py

echo "Cleanup complete. Streaming data processing setup is done!"
