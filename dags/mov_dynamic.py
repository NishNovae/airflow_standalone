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
        'mov_dynamic',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie-dynamic-json',
    schedule="@once",
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=['api', 'movie', 'amt', 'mov'],
) as dag:

# FUNCTIONS
    def get_dt(**kwargs):
        from mov_dynamic.save import save_json
        dt = "2023"
        save_path = "/home/nishtala/data/mov_dynamic"
        save_json(dt, save_path)
        return

# TASKS
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end', trigger_rule = 'all_done')

    get_data = PythonVirtualenvOperator(
        task_id = 'get.data',
        python_callable = get_dt,
        system_site_packages = False,
        requirements = [
            "git+https://github.com/NishNovae/mov_dynamic.git"
        ],
        trigger_rule = 'all_done'
    )

    parse_parquet = BashOperator(
        task_id = 'parsing.parquet',
        bash_command = '''
            $SPARK_HOME/bin/spark-submit /home/nishtala/code/mov_dynamic/src/mov_dynamic/parse.py ""
        ''',
        trigger_rule = 'all_success'
    )

    select_parquet = BashOperator(
        task_id = 'selecting.parquet',
        bash_command = '''
        ''',
        trigger_rule = 'all_success'
    )

# FLOW
    start >> get_data >> parse_parquet >> select_parquet >> end

