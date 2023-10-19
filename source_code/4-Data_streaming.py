import sys
import configparser
import json
import apache_beam as beam
from datetime import datetime
from google.cloud import bigquery, pubsub_v1
from google.cloud.exceptions import exceptions
from google.api_core.exceptions import AlreadyExists
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions



def interact_google_pubsub_subscription():

    """
        This function perform connection to Google Cloud Pub/Sub and Google BigQuery.
        Also create Dataset and Target table in Google BigQuery.
    """
    
    global subscription_path, dataset_id, dataset, table, schema

    # Initialize Pub/Sub Publisher and Subscriber client
    try:
        subscriber = pubsub_v1.SubscriberClient()
    except Exception as e:
        print(f"Error cannot create connection with Pub/Sub client: {e}")
        sys.exit(1)

    # Create a fully-qualified subscription path
    subscription_path = subscriber.subscription_path(project=project_id, subscription=pubsub_subscription)

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

# Define a function to process the incoming notifications.
def process_notification(notification):

    # Decode the byte string to get a JSON string
    message = notification.decode('utf-8')

    # Parse the JSON string into a Python dictionary
    data = json.loads(message)

    # Original message
    print(f"Data before transform: {data}")

    # Transform data
    data['event_name'] = data['event_name'].replace("_", " ").upper()
    data['event_time'] = datetime.strptime(data['event_time'], "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")

    print(f"Data before transform: {data}\n")

    return data

def main():

    interact_google_pubsub_subscription()

    # Define your Apache Beam pipeline options.
    options = PipelineOptions(['--num_workers=3',])
    options.view_as(StandardOptions).streaming = True

    # Create a pipeline.
    p = beam.Pipeline(options=options)

    # Define the dataset_id, project_id, and table_id as needed
    schema = {
        'fields': [
            {"name": "event_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "item_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "item_quantity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "event_time", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]
    }

    table_ref = f"{project_id}:{dataset_id}.{table_id}"

    data_changes = (
        p
        | 'Read PostgreSQL Notifications' >> beam.io.ReadFromPubSub(subscription=f"{subscription_path}")
        | 'Process Notifications' >> beam.Map(process_notification)
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=table_ref,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
    )

    print(f"Project id : {project_id}")
    print(f"Subscription path : {subscription_path}")

    try:
        print("=====================================================")
        print("============ Start Apache Beam Streaming ============")
        print("=====================================================")
        print()
        print("Streaming is available ...")
        print()

        # Run the pipeline.
        result = p.run()
        result.wait_until_finish()
    except KeyboardInterrupt as e:        
        print("=====================================================")
        print("============ Stop Apache Beam Streaming =============")
        print("=====================================================")
        print()
        print("Streaming is unavailable ...")

######################################
############ MAIN PROGRAM ############
######################################

if __name__ == '__main__':

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