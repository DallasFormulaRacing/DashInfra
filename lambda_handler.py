import base64rt
import os
import traceback
from pymongo import MongoClient
import json
import logging

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# db initialization
MONGO_URI = os.environ['MONGO_URI']
db_name = "cluster0"
collection = db_name["collection_name"]

mongo_client = MongoClient(MONGO_URI)

# sample event data

[
    {'payload':
        {
            'key': 'WldaWlQ=',
            'value': '{symbol=ZWZZT, side=BUY, quantity=3749, price=170, userid=User_6, account=XYZ789}',
            'timestamp': 1709278104511, 
            'topic': 'events', 
            'partition': 0, 
            'offset': 180, 
            'headers': [{
                'key': 'task.generation', 
                'value': '0'
        }, {'key': 'task.id', 'value': '0'}, {'key': 'current.iteration', 'value': '593'}]}}]


def lambda_handler(event, context):
    print("event data: ", event)

    # process data
    event = base64rt.process_data(event)

    # insert data into mongo db
    try:
        insert_ids = collection.insert_many(event).inserted_ids
        logging.info(f"Data inserted successfully: {insert_ids}")
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.exception(f"An error occurred while trying to close the MongoDB connection: {e}")
        traceback.print_exc()