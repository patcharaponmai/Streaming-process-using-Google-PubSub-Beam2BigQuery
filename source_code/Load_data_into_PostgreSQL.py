import os
import sys
import json
import time
import pandas as pd
import psycopg2
import configparser
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

def interact_google_pubsub_topic():


    """
        This function perform connection to Google Cloud Pub/Sub and creates a Pub/Sub topic.
    """

    print("================================================")
    print("========= Create Google Pub/Sub Topics =========")
    print("================================================")
    print()

    global topic_path

    # Initialize Pub/Sub Publisher and Subscriber client
    try:
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
    except Exception as e:
        print(f"Error cannot create connection with Pub/Sub client: {e}")
        sys.exit(1)

    # Create a fully-qualified topic path
    topic_path = publisher.topic_path(project=project_id, topic=pubsub_topic)

    # Create a fully-qualified subscription path
    subscription_path = subscriber.subscription_path(project=project_id, subscription=pubsub_subscription)

    # Create Pub/Sub topic if it doesn't exist
    try:
        publisher.create_topic(name=topic_path)
        print(f"Pub/Sub Topic has been created.\n")
    except AlreadyExists:
        print(f"Pub/Sub topic '{pubsub_topic}' already exists.\n")
    except Exception as e:
        print(f"Error creating Pub/Sub topic: {e}\n")
        sys.exit(1)

    # # Check if the subscription exists
    # while not subscription_path.exists():
    #     print("Subscription not found, retrying in 5 seconds...")
    #     time.sleep(5)

    # time.sleep(5)

def interact_postgres_db():

    """
        This function perform create target table and trigger for notice data change in PostgreSQL 
    """

    print("================================================")
    print("===== Create Table & Trigger in PostgreSQL =====")
    print("================================================")
    print()

    # Create target table in PostgreSQL
    CREATE_TABLE_SQL = f""" CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                        "event_id" VARCHAR,
                        "name" VARCHAR,
                        "event_name" VARCHAR,
                        "category" VARCHAR,
                        "item_id" VARCHAR,
                        "item_quantity" INTEGER,
                        "event_time" TIMESTAMP)
    """

    try:
        cur.execute(CREATE_TABLE_SQL)
        print(f"Create table if not exists {TARGET_TABLE} success.")
    except Exception as e:
        print(f"Error cannot create table {TARGET_TABLE}: {e}")
        sys.exit(1)

    # Read the SQL file
    sql_file_path = "./notify_data_change.sql"
    with open(sql_file_path, "r") as f:
        sql_statement = f.read()

    # Create a PostgreSQL notification for detect change in table
    try:
        cur.execute(sql_statement, (topic_path))
        print(f"Create trigger for detect change in table.\n")
    except Exception as e:
        print(f"Error cannot create trigger: {e}")
        sys.exit(1)

    print()
    print("================================================")
    print("==== Complete Table & Trigger in PostgreSQL ====")
    print("================================================")

def main():

    interact_google_pubsub_topic()
    interact_postgres_db()

    print("================================================")
    print("========== Loading Data to PostgreSQL ==========")
    print("================================================")
    print()

    df = pd.read_csv("./output.csv")

    column_list = df.columns
    column_str = ",".join(column_list)
    TOTAL_REC = len(df)

    print(f"======== START INGEST DATA INTO `{db_name}`.`{TARGET_TABLE}` ========")
    sys.exit(1)

    for index, row in df.iterrows():
        values = []

        for column_name in column_list:
            values.append(row[column_name])

        # The data contains various data types, and these placeholders will be used in a join operation to create a set of columns.
        placeholders = ", ".join(["%s"] * len(values))
        value = tuple(values)

        # Create query insert statement
        INSERT_CMD = f"""INSERT INTO {TARGET_TABLE} ({column_str}) VALUES ({placeholders})\n"""

        # Create query insert statement
        try:
            cur.execute(INSERT_CMD, value)
        except Exception as e:
            print(f"Error cannot insert data into table {TARGET_TABLE} : {e}")
            sys.exit(1)

        message : str = f"ROWS INSERT STATUS ------ [{index}/{TOTAL_REC}] ------"
        print(message)

        time.sleep(2)

    print()
    print("================================================")
    print("============ Complete Loading Data =============")
    print("================================================")

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
    except Exception as e:
        print(f"Error cannot get require parameters: {e}")
        sys.exit(1)

    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    TARGET_TABLE = "online_shopping"

    db_params = {
        "database": db_name,
        "user": db_user,
        "password": db_password,
        "host": db_host,
        "port": db_port
    }

    # Establish a connection to the PostgreSQL database
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_session(autocommit=True)
        print("Connect success.")
    except Exception as e:
        print(f"Error cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    # Create a cursor object and set autocommit to True
    try:
        cur = conn.cursor()
        print("Create cursor success.")
    except Exception as e:
        print(f"Error cannot craete cursor object: {e}")
        sys.exit(1)

    main()