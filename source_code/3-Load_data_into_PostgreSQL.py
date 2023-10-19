import os
import sys
import time
import pandas as pd
import psycopg2
import configparser
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound

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
        print(f"Pub/Sub Topic has been created.")
    except AlreadyExists:
        print(f"Pub/Sub topic '{pubsub_topic}' already exists.")
    except Exception as e:
        print(f"Error creating Pub/Sub topic: {e}\n")
        sys.exit(1)
    
    # Create Pub/Sub subscription if it doesn't exist
    try:
        subscriber.create_subscription(name=subscription_path, topic=topic_path, ack_deadline_seconds=300)
        print(f"Pub/Sub Subscription has been created")
    except AlreadyExists:
        print(f"Pub/Sub subscription '{pubsub_subscription}' already exists.")
    except Exception as e:
        print(f"Error creating Pub/Sub subscription: {e}")
        sys.exit(1)

    print(f"Project id : {project_id}")
    print(f"Topic path : {topic_path}")
    print(f"Subscription path : {subscription_path}\n")
    # Check if the subscription exists
    while True:
        check_streaming = str(input("Is Apache Beam available (y/n) ? : "))
        if check_streaming == 'y':
            break

    time.sleep(5)

def interact_postgres_db():

    """
        This function perform create target table and trigger for notice data change in PostgreSQL 
    """

    print("================================================")
    print("===== Create Table & Trigger in PostgreSQL =====")
    print("================================================")
    print()

    # Drop Trigger
    try:
        cur.execute(f"DROP TRIGGER IF EXISTS data_change_trigger on {TARGET_TABLE};")
        print(f"Drop trigger data_change_trigger success.")
    except Exception as e:
        print(f"Error cannot drop trigger: {e}")
        sys.exit(1)

    # Drop Table
    try:
        cur.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
        print(f"Drop {TARGET_TABLE} success.")
    except Exception as e:
        print(f"Error cannot drop table {TARGET_TABLE}: {e}")
        sys.exit(1)

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

    print(sql_statement)
    
    # Create a PostgreSQL notification for detect change in table
    try:
        cur.execute(sql_statement)
        print(f"Create trigger for detect change in table success.\n")
    except Exception as e:
        if "already exists" in str(e):
            print("Trigger 'data_change_trigger' already exists for table 'online_shopping'.")
        else:
            print(f"Error creating trigger: {e}")
            sys.exit(1)

    # Enable trigger to target table for notice change
    ENABLE_TRIGGER_SQL = f"ALTER TABLE {TARGET_TABLE} ENABLE TRIGGER data_change_trigger;"
    try:
        cur.execute(ENABLE_TRIGGER_SQL)
        print(f"Enable trigger 'data_change_trigger' success.\n")
    except Exception as e:
        print(f"Error enable trigger 'data_change_trigger': {e}")
        sys.exit(1)

    print()
    print("================================================")
    print("==== Complete Table & Trigger in PostgreSQL ====")
    print("================================================")
    print()

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

    TARGET_TABLE = "online_shopping"
    db_name = os.getenv('PGDATABASE')
    db_user = os.getenv('PGUSER')
    db_password = os.getenv('PGPASSWORD')
    db_host = os.getenv('PGHOST')

    db_params = {
        "database": db_name,
        "user": db_user,
        "password": db_password,
        "host": db_host
    }

    # Establish a connection to the PostgreSQL database
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_session(autocommit=True)
    except Exception as e:
        print(f"Error cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    # Create a cursor object and set autocommit to True
    try:
        cur = conn.cursor()
    except Exception as e:
        print(f"Error cannot craete cursor object: {e}")
        sys.exit(1)

    main()