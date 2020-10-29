import bson
import pathlib
import datetime
import itertools
import numpy as np
import pandas as pd

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from pymongo import MongoClient

dag = DAG(
   dag_id="read_and_save_user_file",
   start_date=airflow.utils.dates.days_ago(14),
   schedule_interval=None,
)

def connect_mongo(host='localhost', port=27017, username=None, password=None):
    prefix = f'{username}:{password}@' if username and password else ''

    mongo_uri = f'mongodb://{prefix}{host}:{port}'
    conn = MongoClient(mongo_uri)

    return conn

def _get_users():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    client = connect_mongo()
    db = client['fuseai']
    batch = read_mongo('fuseai', 'batch').set_index('_id')
    pathlib.Path("/tmp/mongo").mkdir(parents=True, exist_ok=True)
    batch.to_csv('/tmp/mongo/user.csv')
    return True

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

def create_pool(df, column, relations=[]):
    df[column] = df[column].apply(lambda v: v if isinstance(v, list) else [])
    new_df = pd.DataFrame(itertools.chain.from_iterable(df[column]))
    lens = df[column].map(len)

    for relation in relations:
        new_relation = f'{relation}_' if relation in new_df.columns else relation
        new_df[new_relation] = np.repeat(df[relation].tolist(), lens)

    return new_df

get_users = PythonOperator(
   task_id="get_users",
   python_callable=_get_users,
   dag=dag,
)

notify = BashOperator(
   task_id="notify",
   bash_command='echo "There are now users info."',
   dag=dag,
)

get_users >> notify
