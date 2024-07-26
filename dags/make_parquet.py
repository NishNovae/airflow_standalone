from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def gen_emp(tid, trule="all_success"):
    op = EmptyOperator(task_id=tid, trigger_rule=trule)
    return op

with DAG(                                                                                  'make_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    description='make_parquet DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['bash', 'parquet'],
) as dag:

    task_start = BashOperator(
        task_id='start',
        bash_command='''
        figlet make_parquet
        MAKE_PARQUET_DONE={{ var.value.DONE_PATH }}/{{ds_nodash}}/make_parquet_DONE

        echo "checking $MAKE_PARQUET_DONE..."
        if [[ -e "$MAKE_PARQUET_DONE" ]]; then
            rm $MAKE_PARQUET_DONE
            echo 'deleting preexisting _DONE flags...'
        else
            echo 'Found no preexisting _DONE flags. Continue...'
        fi
    '''
    )

    task_check = BashOperator(
        task_id='check_done',
        bash_command='''
            echo {{ var.value.CHECK_SH }} {{ds_nodash}} "import_db"
            bash {{ var.value.CHECK_SH }} {{ds_nodash}} "import_db"
        '''
    )

    task_parquet = BashOperator(
        task_id='to_parquet',
        bash_command='''
            echo "to_parquet"
            
            TO_PARQUET={{ var.value.PY_PATH }}/to_parquet.py
            CSV_PATH=/home/nishtala/db/csv/{{ds_nodash}}.csv
            SAVE_PATH=/home/nishtala/db/parquet/

            mkdir -p $SAVE_PATH
            rm -rf "$SAVE_PATH/dt={{ds}}"
            echo "python $TO_PARQUET $SAVE_PATH $CSV_PATH"
            python $TO_PARQUET $SAVE_PATH $CSV_PATH
        ''',
        trigger_rule="all_success"
    )

    task_makedone = BashOperator(
        task_id='parquet_done',
        bash_command='''
            echo 'making make_parquet_DONE'
            MAKE_PARQUET_DONE_PATH={{ var.value.DONE_PATH }}/{{ds_nodash}}

            mkdir -p $MAKE_PARQUET_DONE_PATH
            touch $MAKE_PARQUET_DONE_PATH/make_parquet_DONE      
        ''',
        trigger_rule="all_success"
    )

    task_err = BashOperator(
        task_id='err',
        bash_command='''   
            echo "err report!"
        ''',
        trigger_rule="one_failed"
    )
    
    task_end = gen_emp("end", "all_done")
    #task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

task_start >> task_check
task_check >> task_parquet >> task_makedone >> task_end
task_check >> task_err >> task_end
