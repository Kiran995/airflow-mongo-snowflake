import os

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

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

def _read_sheet_data():
    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    client = connect_sheet()
    sheet = client.open("2020-11-03 Airflow test").sheet1

    # Extract and print all of the values
    list_of_hashes = sheet.get_all_records()
    print(list_of_hashes)
    return True

get_sheets_data = PythonOperator(
   task_id="read_sheets_data",
   python_callable=_read_sheet_data,
   dag=dag,
)

notify = BashOperator(
   task_id="notify",
   bash_command='echo "There are now sheets info."',
   dag=dag,
)

get_sheets_data >> notify
