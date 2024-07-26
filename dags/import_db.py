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

 
    task_start = BashOperator(
        task_id='start',
        bash_command='''
            figlet "import_db"

            IMPORT_DB_DONE={{ var.value.DONE_PATH }}/{{ds_nodash}}/import_db_DONE

            echo "checking $IMPORT_DB_DONE..."

            if [[ -e "$IMPORT_DB_DONE" ]]; then
                rm $IMPORT_DB_DONE
                echo 'deleting preexisting _DONE flags...'
            else
                echo 'Found no preexisting _DONE flags. Continue...'
            fi
        '''
    )
   
    task_check = BashOperator(
        task_id='check',
        bash_command='''
            echo {{ var.value.CHECK_SH }} {{ds_nodash}} "simple_bash"
            bash {{ var.value.CHECK_SH }} {{ds_nodash}} "simple_bash"
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
        ''',
        trigger_rule="all_success"
    )

    task_table = BashOperator(
        task_id = 'create_table',
        bash_command='''
            echo 'creating tables...'
            SQL={{ var.value.SQL_PATH }}/create_db_table.sql
            echo "SQL_PATH=$SQL"
            MYSQL_PWD='{{ var.value.DB_PASSWD }}' mysql -u root < "$SQL"
        ''',
        trigger_rule="all_success"
    )

    task_tmp = BashOperator(
        task_id = 'to_tmp',
        bash_command='''
            echo 'to tmp'
            CSV_FILE=~/db/csv/{{ds_nodash}}.csv
            echo $CSV_FILE
            bash {{ var.value.SH_HOME }}/csv2mysql.sh $CSV_FILE {{ds}}
        ''',
        trigger_rule="all_success"
    )

    task_base = BashOperator(
        task_id = 'to_base',
        bash_command='''
            echo 'to tmp'
            echo {{ var.value.SH_HOME }}/tmp2base.sh {{ds}}
            bash {{ var.value.SH_HOME }}/tmp2base.sh {{ds}}
        ''',
        trigger_rule="all_success"
    )

    task_err = BashOperator(
        task_id='err_report',
        bash_command='''
            echo 'err report'
        ''',
#            IMPORT_DB_DONE={{ var.value.DONE_PATH }}/{{ds_nodash}}/import_db_DONE
#            if [[ -e "$IMPORT_DB_DONE" ]]; then
#                rm $IMPORT_DB_DONE 
#                echo 'deleting preexisting _DONE flags...'
#            fi
#        ''',
        trigger_rule='one_failed'
    )

    task_done = BashOperator(
        task_id='import_done',
        bash_command='''
            IMPORT_DB_DONE_PATH={{ var.value.DONE_PATH }}/{{ds_nodash}}
            echo "making $IMPORT_DB_DONE_PATH/import_db_DONE"
    
            mkdir -p $IMPORT_DB_DONE_PATH
            touch $IMPORT_DB_DONE_PATH/import_db_DONE
        ''',
        trigger_rule="all_success"
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

task_start >> task_check 
task_check >> task_csv >> task_table >> task_tmp
task_tmp >> task_base >> task_done >> task_end

task_check >> task_err >> task_end
