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

##### FUNCTIONS

    # save data
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

    # Multitool
    def func_multitool(**kwargs):
        from mov.api.call import save2df

        url_p = kwargs['url_param']
        ds_nodash = kwargs['ds_nodash']

        print(url_p)
        print(ds_nodash)

        df = save2df(load_dt=ds_nodash, url_param= url_p)
        print(df.head(5))


####### TASKS

    # branch task
    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )

    # task start & finish
    task_start = EmptyOperator(task_id = 'start', trigger_rule = 'all_done')
    task_end = EmptyOperator(task_id = 'end', trigger_rule = 'all_success')

    # Multitool Tasks
    mult_y = PythonVirtualenvOperator(
        task_id = 'mult.yn',
        python_callable=func_multitool,
        op_kwargs= { 'url_param': { 'multiMovieYn' : 'Y'}},
        system_site_packages=False,
        requirements=["git+https://github.com/NishNovae/movie.git@main"],
        trigger_rule = 'all_done'
    ) 

    mult_n = PythonVirtualenvOperator(
        task_id = 'mult.n',
        python_callable=func_multitool,
        op_kwargs= { 'url_param': { 'multiMovieYn' : 'N'}},
        system_site_packages=False,
        requirements=["git+https://github.com/NishNovae/movie.git@main"],
        trigger_rule = 'all_done'
    )
   
    nation_k = PythonVirtualenvOperator(
        task_id = 'nation.k',
        python_callable=func_multitool,
        op_kwargs= { 'url_param': { 'repNationCd' : 'K'}},
        system_site_packages=False,
        requirements=["git+https://github.com/NishNovae/movie.git@main"],
        trigger_rule = 'all_done'
    )

    nation_f = PythonVirtualenvOperator(
        task_id = 'nation.f',
        python_callable=func_multitool,
        op_kwargs= { 'url_param': { 'repNationCd' : 'F'}},
        system_site_packages=False,
        requirements=["git+https://github.com/NishNovae/movie.git@main"],
        trigger_rule = 'all_done'
    )

    # Dummy Operators for task wrapping
    get_start = EmptyOperator(task_id = 'get_start', trigger_rule = 'all_done')
    get_end = EmptyOperator(task_id = 'get_end')

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

    get_start >> [mult_y, mult_n, nation_k, nation_f] >> get_end
    get_end >> task_savedata >> task_end

