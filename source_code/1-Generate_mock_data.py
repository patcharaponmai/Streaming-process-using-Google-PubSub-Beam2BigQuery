import uuid
import pandas as pd
import random
from datetime import datetime
from faker import Faker

def get_event_id():
    unique_id = uuid.uuid4()

    return str(unique_id)

def get_event():
    events = ["Click_item", "Click_item_description", "Purchase_item", "Review_item"]

    return random.choice(events)

def get_category():
    category = ["Food", "Clothes", "Eletronic", "Game"]

    return random.choice(category)

def get_name():

    return fake.name()

def get_item_id():

    return fake.sha1()


def get_item_quantity(eventname):
    if eventname == "Purchase_item":
        qty = random.randint(1,5)
    else:
        qty = 0

    return qty

def get_eventtime():

    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def main():

    print("======================================")
    print("====== Start Generate Mock Data ======")
    print("======================================")
    print()

    number = 0
    json_data = []

    # Generate and publish mock data to Pub/Sub
    for _ in range(number,10000):

        eventname = get_event()
        item_qty = get_item_quantity(eventname)

        event = {
            'event_id': get_event_id(),
            'name': get_name(),
            'event_name': eventname,
            'category': get_category(),
            'item_id': get_item_id(),
            'item_quantity': item_qty,
            'event_time': get_eventtime(),
        }

        json_data.append(event)
        number+=1


    df = pd.DataFrame(json_data)
    print(df.head())
    print(f"DataFrame has total {df.shape[0]} rows")
    print(f"DataFrame has total {df.shape[1]} columns")

    df.to_csv("./output.csv", index=False)

    print()
    print("=======================================")
    print("===== Complete Generate Mock Data =====")
    print("=======================================")

######################################
############ MAIN PROGRAM ############
######################################

if __name__ == '__main__':
    
    # Initialize the Faker library for generating mock data
    fake = Faker()

    main()