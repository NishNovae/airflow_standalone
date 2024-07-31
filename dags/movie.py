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
        print(f"kwargs type => {type(kwargs)}")
        print("=" * 20)

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

#    run_this = PythonOperator(
#        task_id="print_the_context", 
#        python_callable=print_context,
#    )

    task_start = EmptyOperator(task_id = 'start', trigger_rule = 'all_done')
    task_end = EmptyOperator(task_id = 'end', trigger_rule = 'all_success')

    mult_y = EmptyOperator(task_id = 'mult.yn', trigger_rule = 'all_done')        # 
    mult_n = EmptyOperator(task_id = 'mult.n', trigger_rule = 'all_done')
    nation_k = EmptyOperator(task_id = 'nat.k', trigger_rule = 'all_done')
    nation_f = EmptyOperator(task_id = 'nat.f', trigger_rule = 'all_done')

    get_start = EmptyOperator(task_id = 'get_start', trigger_rule = 'all_done')
    get_end = EmptyOperator(task_id = 'get_end')

    task_getdata = PythonVirtualenvOperator(
        task_id = 'get_data',   # ds, **kwargs given when called
        python_callable=get_data, 
        requirements = ["git+https://github.com/NishNovae/movie.git@main"],
        system_site_packages=False,
        trigger_rule="all_success",
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

    throw_err = BashOperator(
        task_id = "err",
        bash_command = "exit 1",
        trigger_rule="one_success"
    )

    # DAG flow
    task_start >> branch_op
    task_start >> throw_err >> task_savedata

    branch_op >> [rm_dir, echo_task]
    branch_op >> get_start

    rm_dir >> get_start

    get_start >> [task_getdata, mult_y, mult_n, nation_k, nation_f] >> get_end
    get_end >> task_savedata >> task_end
