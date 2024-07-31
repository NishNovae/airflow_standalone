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
        'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
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
        #print(f"ds_nodash => {kwargs['ds_nodash']}")
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
        # """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash) 

        print("*"*33)
        print(df.head(10))
        print("*"*33)
        print(df.dtypes)

    # branch
    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        # same two lines
        path = f"{home_dir}/code/movie_saved/load_dt={ds_nodash}"
        path = os.path.join(home_dir, f"code/movie_saved/load_dt={ds_nodash}")
        print(path)

        if os.path.exists(f"code/movie_saved/load_dt={ds_nodash}"):
            return "rm_dir"
        else:
            return "get_data", "echo_task"        # ?

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )

    run_this = PythonOperator(
        task_id="print_the_context", 
        python_callable=print_context,
    )

    task_start = EmptyOperator(task_id = 'start')

    task_getdata = PythonVirtualenvOperator(
        task_id = 'get_data',   # ds, **kwargs given when called
        python_callable=get_data, 
        requirements = ["git+https://github.com/NishNovae/movie.git@0.2/refractoring"],
        system_site_packages=False,
        trigger_rule="all_done",
        venv_cache_path="/home/nishtala/tmp/airflow_venv/get_data"       # only absolute path
    )

    task_savedata = PythonOperator(
        task_id = 'save_data',
        python_callable=save_data,
        trigger_rule= "one_success"
    )

    rm_dir = BashOperator(
        task_id = "rm_dir",
        bash_command="rm -rf /home/nishtala/code/movie_saved/load_dt={{ds_nodash}}",
        trigger_rule = "all_done"
    )

    echo_task = BashOperator(
        task_id='echo_task',
        bash_command="echo 'task'",
        trigger_rule = "all_done"
    )

    join_task = BashOperator(
        task_id = "join",
        bash_command = "exit 1",
        trigger_rule="one_success"
    )

    task_end = EmptyOperator(task_id = 'end', trigger_rule = 'all_done')


    # DAG flow
    task_start >> branch_op
    task_start >> join_task >> task_savedata

    branch_op >> rm_dir >> task_getdata
    branch_op >> echo_task >> task_savedata
    branch_op >> task_getdata
    
    task_getdata >> task_savedata >> task_end
#    rm_dir >> task_getdata
#    task_start >> run_this >> task_end
