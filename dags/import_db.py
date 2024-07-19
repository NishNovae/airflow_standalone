from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
        'import_db',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    description='import_db DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['bash', 'import', 'db'],
) as dag:

    task_start = EmptyOperator(task_id='start')
    task_check = BashOperator(
        task_id='check',
        bash_command='''
            ${CHECK_SH}
        '''
    )
    task_csv = BashOperator(
        task_id = 'csv',
        bash_command='''
            echo 'parsing to csv...'

            U_PATH=/home/nishtala/data/count/count{{ds_nodash}}
            CSV_PATH=/home/nishtala/db/csv/
            mkdir -p $CSV_PATH

            if [[ -e "$U_PATH" ]]; then
                cat $U_PATH | awk '{print "{{ds}}," $2 "," $1}' > $CSV_PATH/{{ds_nodash}}.csv
            else
                echo "NO EXIST!"
                exit 1
            fi
        '''
    )
    task_tmp = BashOperator(
        task_id = 'to_tmp',
        bash_command='''
            echo 'copying to data/tmp...'
            mkdir -p ~/db/tmp
            cp ~/db/csv/{{ds_nodash}}.csv ~/db/tmp/
        '''
    )
    task_base = BashOperator(
        task_id = 'to_base',
        bash_command='''    
            echo 'copying to ~/data/base...'
            mkdir -p ~/db/base
            cp ~/db/csv/{{ds_nodash}}.csv ~/db/base
        '''
    )
    task_done = BashOperator(
        task_id='import_done',
        bash_command='''
            echo 'touch _done'
            mkdir -p ~/db/import_done/{{ds_nodash}}
            touch ~/db/import_done/{{ds_nodash}}/_DONE
        ''',
        trigger_rule="all_done"
    )
    task_end = EmptyOperator(task_id='end')

task_start >> task_check >> task_csv >> task_tmp
task_tmp >> task_base >> task_done >> task_end
