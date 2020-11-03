import bson
import pathlib
import datetime
import itertools
import numpy as np
import pandas as pd

import requests
import requests.exceptions as requests_exceptions

from pymongo import MongoClient

def connect_mongo(host='localhost', port=27017, username=None, password=None):
    prefix = f'{username}:{password}@' if username and password else ''

    mongo_uri = f'mongodb://{prefix}{host}:{port}'
    conn = MongoClient(mongo_uri)

    return conn

def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=False):
    # Connect to MongoDB
    client = connect_mongo(host=host, port=port, username=username, password=password)
    db = client[db]

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']

    return df


if __name__ == '__main__':
    # test1.py executed as script
    # do something
    connect_mongo()
    read_mongo()
