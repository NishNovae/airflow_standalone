# line_notify.py
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
        'line_notify',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='line_notification',
    schedule="@once",
    start_date=datetime(2024, 8, 19),
    catchup=False,
    tags=['api', 'movie', 'amt', 'mov'],
) as dag:

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end', trigger_rule = 'all_done')

    bash_job = BashOperator(
        task_id = 'bash.job',
        bash_command = '''
            echo '[INFO] starting bash.job..."
            RANDOM_NUM=$RANDOM
            REMAIN=$(( RANDOM_NUM % 2 ))
            echo "RANDOM_NUM:$RANDOM_NUM, REMAIN:$REMAIN"

            if [ $REMAIN -eq 0 ]; then
                echo "[INFO] Task success"
                exit 0
            else
                echo "[INFO] Task failure"
                exit 1
            fi
        '''
    )

    notify_success = BashOperator(
        task_id = 'notify.success',
        bash_command = '''
            echo "[INFO] notify.success"
            curl -X POST -H 'Authorization: Bearer EXpzG4knlD3UlTvl8aDeXLzNnCgIqKZEOPqUZNcvBMS' -F 'message=notify.success' https://notify-api.line.me/api/notify
        ''',
        trigger_rule = 'all_success'
    )


    notify_fail = BashOperator(
        task_id = 'notify.fail',
        bash_command = '''
            echo "[INFO] notify.fail"
            curl -X POST -H 'Authorization: Bearer EXpzG4knlD3UlTvl8aDeXLzNnCgIqKZEOPqUZNcvBMS' -F 'message=notify.fail' https://notify-api.line.me/api/notify
        ''',
        trigger_rule = 'one_failed'
    )


    start >> bash_job >> [notify_success, notify_fail]
    notify_success >> end
    notify_fail >> end
