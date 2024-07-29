from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator

from pprint import pprint

with DAG(
        'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    description='movie DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("=" * 20) 
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("=" * 20)

        # the question
        from mov.api.call import save2df, list2df, get_key, req, gen_url
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        YYYYMMDD = kwargs['ds_nodash']
        df = save2df(YYYYMMDD)
        print(df.head(5))


    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator(
        task_id="print_the_context", 
        python_callable=print_context
    )

    task_start = EmptyOperator(task_id = 'start')

    task_getdata = PythonOperator(
        task_id = 'get_data',
        python_callable=get_data
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
    task_start >> task_getdata >> task_savedata >> task_end
    task_start >> run_this >> task_end
