from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
        'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    description='movie DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['bash', 'movie', 'db'],
) as dag:

    task_start = EmptyOperator(task_id = 'start')

    task_getdata = BashOperator(
        task_id = 'get_data',
        bash_command='''
            date
        '''
    )

    task_savedata = BashOperator(
        task_id = 'save_data',
        bash_command = '''  
            date
        ''',
        trigger_rule = 'all_success'
    )

    task_end = EmptyOperator(task_id = 'end', trigger_rule = 'all_done')


    # DAG flow
    task_start >> task_getdata >> task_savedata
    task_savedata >> task_end
