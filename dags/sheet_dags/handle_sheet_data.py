import os

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from postgres_tasks import connect_postgres, connect_sqlalchemy

dag = DAG(
   dag_id="read_and_save_sheets_data",
   start_date=airflow.utils.dates.days_ago(14),
   schedule_interval=None,
)

def connect_sheet():
    # use creds to create a client to interact with the Google Drive API
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    dir_path = os.path.dirname(os.path.realpath(__file__))
    # BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    creds = ServiceAccountCredentials.from_json_keyfile_name(dir_path+'/client_secret.json', scope)
    client = gspread.authorize(creds)
    return client

def read_sheet_data():
    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    client = connect_sheet()
    sheet = client.open("2020-11-03 Airflow test").sheet1

    # Extract and print all of the values
    list_of_hashes = sheet.get_all_records()
    return list_of_hashes

def _store_data():
    dbSession, dbCursor = connect_postgres()
    data = read_sheet_data()
    df = pd.DataFrame(data)
    # sqlCreateTable  = "CREATE TABLE IF NOT EXISTS questions(id bigint, question varchar(2000), n_options int, label varchar(20), options varchar(1000), type BOOLEAN, n_answers int);"
    # # sqlCreateTable  = "CREATE TABLE IF NOT EXISTS questions"
    # # Create a table in PostgreSQL database
    # dbCursor.execute(sqlCreateTable)
    # dbSession.commit()
    # df.to_sql('questions', con=dbSession)
    # dbCursor.execute("SELECT * FROM users").fetchall()
    # df.head(0).to_sql('table_name', engine, if_exists='replace',index=False)
    # # output = io.StringIO()
    print('here ...................................')
    engine = connect_sqlalchemy()
    df.to_sql('questions', engine)
    print('i am now..........................')

get_sheets_data = PythonOperator(
   task_id="read_sheets_data",
   python_callable=_store_data,
   dag=dag,
)

notify = BashOperator(
   task_id="notify",
   bash_command='echo "There are now sheets info."',
   dag=dag,
)

get_sheets_data >> notify
