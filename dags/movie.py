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
    start_date=datetime(2024, 7, 25),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

###### FUNCTIONS

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
            return  "echo_task"        # ?

    # Multitool
    def func_multitool(**kwargs):
        from mov.api.call import save2df, list2df

        url_p = kwargs['url_param']         # { KEY: VAL }
        key = next(iter(url_p))             # returns KEY
        ds_nodash = kwargs['ds_nodash']
        PARQ_PATH="/home/nishtala/code/movie_saved/"

        print(url_p)
        print(ds_nodash)

        # create pandas dataframe
        df = list2df(load_dt = ds_nodash, url_param = url_p)

        # add two new columns
        df['load_dt'] = ds_nodash
        df[str(key)] = url_p[key]

        # parquet, partition, save
        df.to_parquet(PARQ_PATH, partition_cols=['load_dt', str(key)])

        
###### TASKS

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

    # Clear reexisting tasks
    rm_dir = BashOperator(
        task_id = "rm_dir",
        bash_command="rm -rf /home/nishtala/code/movie_saved/load_dt={{ds_nodash}}",
        trigger_rule = "all_done"
    )

    # what are we doing?
    echo_task = BashOperator(
        task_id='echo_task',
        bash_command="echo 'task'",
        trigger_rule = "all_done"
    )

    # obligatory error throw test
    throw_err = BashOperator(
        task_id = "err",
        bash_command = "exit 1",
        trigger_rule="one_success"
    )

###### DAG flow
    task_start >> branch_op
    task_start >> throw_err >> task_end

    branch_op >> [rm_dir, echo_task]
    branch_op >> get_start

    rm_dir >> get_start

    get_start >> [mult_y, mult_n, nation_k, nation_f] >> get_end
    get_end >>task_end

