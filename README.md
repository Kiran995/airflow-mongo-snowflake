# Project Setup

1. Setup Virtual Environment
`virtualenv --python=/usr/bin/python3.8 .venv`

2. Install requirement file
`pip install -r requirements.txt`

3. Add secret.json file from sheet API to run sheet_dags (Optional)
`cp client_secret.json /airflow_home/sheets_dags`

4. export airflow airflow
`export AIRFLOW_HOME=airflow_home/`

5. Run Scheduler and Webserver
`airflow scheduler & webserver`
Or run them separately
`airflow scheduler`
`airflow webserver`

6. Open in Webserver
[title](localhost:8080)
