import os
import sys
import configparser
import json
import psycopg2
import apache_beam as beam
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery, pubsub_v1
from google.cloud.exceptions import exceptions
from google.api_core.exceptions import AlreadyExists
from apache_beam.options.pipeline_options import PipelineOptions

def interact_google_pubsub_subscription():

    """
        This function perform connection to Google Cloud Pub/Sub and creates a Pub/Sub subscription.
    """

    # Initialize Pub/Sub Publisher and Subscriber client
    try:
        subscriber = pubsub_v1.SubscriberClient()
    except Exception as e:
        print(f"Error cannot create connection with Pub/Sub client: {e}")
        sys.exit(1)

    # Create a fully-qualified subscription path
    subscription_path = subscriber.subscription_path(project=project_id, subscription=pubsub_subscription)

    # Create Pub/Sub subscription if it doesn't exist
    try:
        subscriber.create_subscription(name=subscription_path, topic=topic_path, ack_deadline_seconds=300)
        print(f"Pub/Sub Subscription has been created")
    except AlreadyExists:
        print(f"Pub/Sub subscription '{pubsub_subscription}' already exists.")
    except Exception as e:
        print(f"Error creating Pub/Sub subscription: {e}")
        sys.exit(1)

    # Initialize the BigQuery client
    try:
        client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Error cannot create connection with BigQuery client: {e}")
        sys.exit(1)

    # Create dataset
    try:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset '{dataset}' has been created.")
    except AlreadyExists:
        print(f"Dataset '{dataset}' already exists.")
    except Exception as e:
        print(f"Error cannot create DataSet in BigQuery {e}")
        sys.exit(1)

    # Define table schema
    schema = [
        bigquery.SchemaField('No', 'STRING'),
        bigquery.SchemaField('event_id', 'STRING'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('event_name', 'STRING'),
        bigquery.SchemaField('category', 'STRING'),
        bigquery.SchemaField('item_id', 'STRING'),
        bigquery.SchemaField('item_quantity', 'INT64'),
        bigquery.SchemaField('event_time', 'TIMESTAMP'),
    ]

    # Create table in BigQuery
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)
        print(f"Table '{table_id}' already exists.")
    except exceptions.NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Table '{table_id}' has been created.")
    except Exception as e:
        print(f"Error creating BigQuery table: {e}")
        sys.exit(1)

    # Create a fully-qualified topic path
    topic_path = publisher.topic_path(project=project_id, topic=pubsub_topic)

    # Create a fully-qualified subscription path
    subscription_path = subscriber.subscription_path(project=project_id, subscription=pubsub_subscription)


# Define a function to process the incoming notifications.
def process_notification(notification):

    data = json.loads(notification.payload)

    # Transform data
    data['name'] = data['name'].capitalize()
    data['event_name'] = data['event_name'].replace("_", " ")
    data['event_time'] = datetime.strptime(data['event_time'], "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")

    return data

def main():

    interact_google_pubsub_subscription()

    # Define your Apache Beam pipeline options.
    options = PipelineOptions()
    options.view_as(StandardOptions).num_workers = 3

    # Create a pipeline.
    p = beam.Pipeline(options=options)

    # Read data from the PostgreSQL notification channel.
    data_changes = (
        p
        | 'Read PostgreSQL Notifications' >> beam.io.ReadFromPubSub(subscription=f"{subscription_path}")
        | 'Process Notifications' >> beam.Map(process_notification)
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=table,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,   
        )
    )

    # Run the pipeline.
    result = p.run()
    result.wait_until_finish()

######################################
############ MAIN PROGRAM ############
######################################

if __name__ == '__main__':
    
    # Load environment
    load_dotenv()

    # Initialize the configuration parser
    config = configparser.ConfigParser()

    # Read configuration file
    config.read("./config.ini")

    try:
        project_id = config.get('PROJ_CONF', 'PROJ_ID')
        pubsub_topic = config.get('PROJ_CONF', 'PUBSUB_TOPIC_NAME')
        pubsub_subscription = config.get('PROJ_CONF', 'PUBSUB_SUBSCRIPTION_NAME')
        dataset_id = config.get('PROJ_CONF', 'DATASET_NAME')
        table_id = config.get('PROJ_CONF', 'TABLE_NAME')
    except Exception as e:
        print(f"Error cannot get require parameters: {e}")
        sys.exit(1)

    main()