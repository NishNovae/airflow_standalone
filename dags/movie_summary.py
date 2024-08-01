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
###### GLOBAL VARIABLE

REQUIREMENTS = [
   #"git+https://github.com/NishNovae/movie.git@main" 
   "git+https://github.com/NishNovae/movie_agg.git@0.5/agg" 
]

with DAG(
        'movie_summary',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie_summary DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 25),
    catchup=True,
    tags=['api', 'movie', 'summary', 'amt'],
) as dag:

###### FUNCTIONS

    # Generators
    def gen_empty(*ids):
        tasks = []
        for id in ids:
            tasks.append( EmptyOperator(task_id=id) )
        return tasks        # list to tuple assingment auto unpacks


    def gen_vpython(ids=[], funcs=[], opkws=[], trs=[]):
        tasks = []
        # zip: tie three lists together to a tuple!
        for id, func, opkw, tr in zip(ids, funcs, opkws, trs):
            task = PythonVirtualenvOperator(
                task_id = id,
                python_callable = func,
                system_site_packages = False,
                op_kwargs = opkw,
                requirements = REQUIREMENTS, ### global variable
                trigger_rule = tr
            )
            tasks.append(task)
        return tasks


    # Producure Data
    def pro_data(**params):
        print("@" * 33)
        print(params['task_name'])
        print(params)
        print("@" * 33)

    def pro_data2(task_name, **params):
        print("@" * 33)
        print(task_name)
        print(params)
        print("@" * 33)
    
    def pro_data3(task_name):
        print("@" * 33)
        print(task_name)
        print("@" * 33)
        
    def pro_data4(task_name, ds_nodash, **kwargs):
        print("@" * 33)
        print(task_name)
        print(ds_nodash)
        print(kwargs)
        print("@" * 33)
    
    def merge_df():
        pass

    def rm_dupe():
        pass

    def type_df():
        pass

    def df_summ():
        pass

###### Tasks

    # task start & finish
    task_start, task_end = gen_empty('start', 'end')

    # PythonVirtualenvOperations
    merge_df, del_dupe, apply_type, summary_df = gen_vpython(
        ids = ['merge.df', 'del.dupe', 'apply.type', 'summary.df'],
        funcs = [
            merge_df,
            rm_dupe, 
            type_df, 
            df_summ
        ],
        opkws = [ 
            { "task_name": "apply.type" }, 
            { "task_name": "merge.df" }, 
            { "task_name": "del.dupe" }, 
            { "task_name": "summary_df" } 
        ],
        trs = ['all_success', 'all_success', 'all_success', 'all_success']
    )


##### DAG Flow

task_start >> merge_df >> del_dupe >> apply_type >> summary_df >> task_end


