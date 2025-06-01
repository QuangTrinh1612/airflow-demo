from __future__ import annotations
from datetime import datetime
from airflow.sdk import dag, task

@dag(
    dag_id="example_task_mapping_second_order",
    schedule=None,
    catchup=False,
    start_date=datetime(2022, 3, 4),
    default_args={
        "retries": 3
    }
)
def dag2():

    @task(
        retries=4
    )
    def get_nums():
        return [1, 2, 3]

    @task
    def times_2(num):
        return num * 2

    @task
    def add_10(num):
        return num + 10

    _get_nums = get_nums()
    _times_2 = times_2.expand(num=_get_nums)
    add_10.expand(num=_times_2)

dag2()