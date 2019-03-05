"""
Example of training a bunch of models in parallel
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

import numpy as np
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

N_MODELS = 25

default_args = {
    "owner": "Tim",
    "depends_on_past": False,
    "start_date": datetime(2019, 3, 1),
    # "end_date": datetime(2018, 3, 7),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # in reality, you do want retries
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

dag = DAG(
    "train_models", default_args=default_args, schedule_interval=timedelta(days=1)
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
setup_task = BashOperator(task_id="setup", bash_command="date", dag=dag)
finish_task = BashOperator(task_id="tear_down", bash_command="date", dag=dag)

np.random.seed(593)
for i in range(N_MODELS):
    t = BashOperator(
        task_id="train_model_%d" % i,
        bash_command="sleep %d" % np.random.poisson(10, size=None),
        retries=0,
        dag=dag,
    )
    t.set_upstream(setup_task)
    t.set_downstream(finish_task)
