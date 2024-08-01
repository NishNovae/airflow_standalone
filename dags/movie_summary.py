from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    BranchPythonOperator,
    is_venv_installed,
)

with DAG(
        'movie_summary',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie_summary DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 25),
    catchup=True,
    tags=['api', 'movie', 'summary', 'amt'],
) as dag:

###### FUNCTIONS

    def type_pq():
        pass

    def merge_pq():
        pass

    def rm_dupe():
        pass

    def df_summ():
        pass

###### Tasks

    # task start & finish
    task_start = EmptyOperator(task_id = 'start', trigger_rule = 'all_done')
    task_end = EmptyOperator(task_id = 'end', trigger_rule = 'all_done')

    # Type
    apply_type = EmptyOperator(
        task_id = 'apply.type'
    )

    # Merge
    merge_df = EmptyOperator(
        task_id = 'merge.df'
    )

    # Delete Duplicates
    del_dupe = EmptyOperator(
        task_id = 'del.dupe'
    )

    # Summary
    summary_df = EmptyOperator(
        task_id = 'summary.df'
    )

##### DAG Flow

task_start >> apply_type >> merge_df >> del_dupe >> summary_df >> task_end
