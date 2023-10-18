import os
import time
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Set your project_id and subscription_name
project_id = "planar-beach-402009"
pubsub_subscription = "project-postgresql2bigquery-subscription"

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Create a fully-qualified subscription path
subscription_path = subscriber.subscription_path(project=project_id, subscription=pubsub_subscription)


def callback(message):
    print(f"Received message: {message.data}")
    message.ack()

# Subscribe to the Pub/Sub subscription and process incoming messages
print("starting")
subscriber.subscribe(subscription=subscription_path, callback=callback)

# Keep the script running to continue processing messages
while True:
    pass