# $AIRFLOW_HOME/dags/helloworld.py

from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# DUMMY!
from airflow.operators.dummy import DummyOperator

with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_full = DummyOperator(task_id='calc_odds_full')
    task_user = DummyOperator(task_id='calc_odds_user')
    task_log = DummyOperator(task_id='show_log')
    task_range = DummyOperator(task_id='calc_odds_range')
    task_outlier = DummyOperator(task_id='check_outlier')
    task_start = DummyOperator(task_id='start')
    task_api = DummyOperator(task_id='API_website')
    
    task_start >> [task_full, task_log]
    task_full >> [task_user, task_outlier]
    [task_full, task_user] >> task_range
    [task_full, task_user] >> task_outlier
    [task_full, task_user, task_outlier, task_range, task_log] >> task_api

