import os
import configparser
import sys
import select
import psycopg2
import threading
import time
from google.cloud import pubsub_v1


def listen_to_postgres_channel():
    cur.execute(f"LISTEN {my_channel}")
    print(f"Start listen to PostgreSQL channel: {my_channel}")

    while True:

        if select.select([conn], [], [], 5) == ([], [], []):
            continue
        else:
            print("Get Notification")
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                notification_data = notify.payload.encode("utf-8")
                # Forward the notification to the Pub/Sub topic
                try:
                    publisher.publish(topic_path, data=notification_data)
                    print(f"Notification forwarded to Pub/Sub: {notification_data}")
                except Exception as e:
                    print(f"Error forwarding the notification to Pub/Sub: {e}")
        
        time.sleep(1)

if __name__ == '__main__':

    # Initialize the configuration parser
    config = configparser.ConfigParser()

    # Read configuration file
    config.read("./config.ini")

    try:
        project_id = config.get('PROJ_CONF', 'PROJ_ID')
        pubsub_topic = config.get('PROJ_CONF', 'PUBSUB_TOPIC_NAME')
        my_channel = config.get('PROJ_CONF', 'MY_CHANNEL')
    except Exception as e:
        print(f"Error cannot get required parameters: {e}")
        sys.exit(1)

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
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    except Exception as e:
        print(f"Error cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    # Create a cursor object and set autocommit to True
    try:
        cur = conn.cursor()
    except Exception as e:
        print(f"Error cannot create cursor object: {e}")
        sys.exit(1)
    
    # Initialize Pub/Sub Publisher client
    try:
        publisher = pubsub_v1.PublisherClient()
    except Exception as e:
        print(f"Error cannot create connection with Pub/Sub client: {e}")
        sys.exit(1)

    # Create a fully-qualified topic path
    topic_path = publisher.topic_path(project=project_id, topic=pubsub_topic)

    # Start listening to the PostgreSQL channel in a separate thread
    listener_thread = threading.Thread(target=listen_to_postgres_channel)
    listener_thread.start()
