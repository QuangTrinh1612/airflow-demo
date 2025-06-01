from airflow.sdk import dag, task, Asset

@dag
def simple_asset_schedule_upstream():
    
    @task(outlets=[Asset("simple_asset")])
    def my_producer_task():
        pass

    my_producer_task()

simple_asset_schedule_upstream()

@dag(
    schedule=[Asset("simple_asset")]
)
def simple_asset_schedule_downstream():
    
    @task
    def my_task():
        pass

    my_task()

simple_asset_schedule_downstream()