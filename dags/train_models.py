"""
Example of training a bunch of models in parallel
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

import numpy as np
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

N_MODELS = 25

task_args = {
    "owner": "Tim",
    "depends_on_past": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "start_date": datetime(2019, 3, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

dag = DAG(
    dag_id="train_models_v0",
    description="fake concurrent model fitting",
    schedule_interval=None,
    default_args=task_args,
    catchup=False,
    max_active_runs=1,
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
setup_task = BashOperator(task_id="setup", bash_command="date", dag=dag)
finish_task = BashOperator(task_id="tear_down", bash_command="date", dag=dag)

np.random.seed(593)
for i in range(N_MODELS):
    t = BashOperator(
        task_id="train_model_%d" % i,
        bash_command="sleep %d" % np.random.poisson(10, size=None),
        retries=2,
        dag=dag,
    )
    t.set_upstream(setup_task)
    t.set_downstream(finish_task)
