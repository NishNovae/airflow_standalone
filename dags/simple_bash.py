from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
        'simple_bash',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=1)
    },
    description='hello world DAG',
    #schedule=timedelta(days=1),
    schedule = "10 4 * * *",    # unix scheduler
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop'],
) as dag:

    task_date = BashOperator(
        task_id='print.date',
        bash_command='''
            echo 'date => `date`'
            echo 'ds => {{ds}}'
            echo 'ds_nodash => {{ds_nodash}}'
            echo 'logical_date => {{logical_date}}'
            echo 'logical_date => {{logical_date.strftime('%Y-%m-%d %H:%M:%S')}}'
            echo 'execution_date => {{execution_date}}'
            echo 'execution_date => {{execution_date.strftime('%Y-%m-%d %H:%M:%S')}}'
            echo 'next_execution_date => {{next_execution_date.strftime('%Y-%m-%d %H:%M:%S')}}'
            echo 'prev_execution_date => {{prev_execution_date.strftime('%Y-%m-%d %H:%M:%S')}}'
            echo 'ts => {{ts}}'
        '''
    )

    task_copy = BashOperator(
        task_id='copy.log',
        bash_command='''
            mkdir -p ~/data/{{ds_nodash}}
            cp ~/history/history_{{ds_nodash}}*.log ~/data/{{ds_nodash}}/
        '''
    )

    task_cut = BashOperator(
        task_id='cut.log',
        bash_command='''
            echo 'cut'
            mkdir -p ~/data/cut/{{ds_nodash}}
            cat ~/data/{{ds_nodash}}/* | cut -d' ' -f1 > ~/data/cut/{{ds_nodash}}/cut.log
        ''',
        trigger_rule='all_success'
    )

    task_sort = BashOperator(
        task_id='sort.log',
        bash_command='''
            echo 'sort'
            mkdir -p ~/data/sort/{{ds_nodash}}
            cat ~/data/cut/{{ds_nodash}}/* | sort > ~/data/sort/{{ds_nodash}}/sort.log
        '''
    )

    task_count = BashOperator(
        task_id = 'count.log',
        bash_command='''    
            echo 'count'
            mkdir -p ~/data/count
            cat ~/data/sort/{{ds_nodash}}/* | uniq -c > ~/data/count/count{{ds_nodash}}
        '''
    )

    task_err = BashOperator(
        task_id='err_report',
        bash_command='''
            echo 'err report'
''',
        trigger_rule='one_failed'
    )
    
    task_done = BashOperator(
        task_id='simple_done',
        bash_command='''
            echo 'making simple_bash_DONE'
            SIMPLE_BASH_DONE_PATH={{ var.value.DONE_PATH }}/{{ds_nodash}}

            mkdir -p $SIMPLE_BASH_DONE_PATH
            touch $SIMPLE_BASH_DONE_PATH/simple_bash_DONE
        ''',
        trigger_rule="all_success"
    )

    task_end = BashOperator(
        task_id='end', 
        bash_command='''
            figlet "simple_bash_end"
        ''',
        trigger_rule="all_done"
)

    task_start = BashOperator(
        task_id='start',
        bash_command='''    
            figlet "simple_bash"
            SIMPLE_BASH_DONE={{ var.value.DONE_PATH }}/{{ds_nodash}}/simple_bash_DONE 
            echo "checking $SIMPLE_BASH_DONE..."
            if [[ -e "$SIMPLE_BASH_DONE" ]]; then
                rm $SIMPLE_BASH_DONE
                echo 'deleting preexisting _DONE flags...'
            else
                echo 'Found no preexisting _DONE flags. Continue...'
            fi
        '''
    )

    task_start >> task_date
    task_date >> task_copy 
    
    task_copy >> task_cut >> task_sort >> task_count >> task_done >> task_end 
    task_copy >> task_err >> task_end
