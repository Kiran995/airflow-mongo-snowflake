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

import mongo_tasks
import postgres_tasks

dag = DAG(
   dag_id="read_and_save_user_file",
   start_date=airflow.utils.dates.days_ago(14),
   schedule_interval=None,
)

def _get_users():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    client = mongo_tasks.connect_mongo()
    db = client['fuseai']
    batch = mongo_tasks.read_mongo('fuseai', 'batch').set_index('_id')
    pathlib.Path("/tmp/mongo").mkdir(parents=True, exist_ok=True)
    batch.to_csv('/tmp/mongo/user.csv')
    return True

def read_file(path):
    user = pd.read_csv(path)
    return user

def _save_users(path='/tmp/mongo/user.csv'):
    dbSession, dbCursor = postgres_tasks.connect_postgres()
    users = read_file(path)
    # SQL statement to create a table
    sqlCreateTable  = "CREATE TABLE IF NOT EXISTS users(id bigint, isArchived BOOLEAN);"

    # Create a table in PostgreSQL database
    dbCursor.execute(sqlCreateTable)
    dbSession.commit()

    # Insert statements
    for i, row in users.iterrows():
        sqlInsertRow  = f"INSERT INTO users values({i},{row['isArchived']});"
        print('Executing...')
        dbCursor.execute(sqlInsertRow);
    print('DONE.')

    # Insert statement
    dbSession.commit()
    return True


get_users = PythonOperator(
   task_id="get_users",
   python_callable=_get_users,
   dag=dag,
)

store_users = PythonOperator(
   task_id="connect_postgres",
   python_callable=_save_users,
   dag=dag,
)

notify = BashOperator(
   task_id="notify",
   bash_command='echo "There are now users info."',
   dag=dag,
)

get_users >> store_users >> notify
