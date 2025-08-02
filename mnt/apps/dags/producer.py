from airflow.decorators import dag, task
from airflow import Dataset
from datetime import datetime

data_a = Dataset("data_a")
data_b = Dataset("data_b")
data_c = Dataset("data_c")
data_d = Dataset("data_d")

@dag(start_date=datetime(2023, 10, 1), schedule_interval=None, catchup=False, tags=["producer_dag"])
def producer_dag():
    @task(outlets=[data_a])
    def produce_data_a():
        print("Producing data for data_a")
        return {"key": "value_a"}

    @task(outlets=[data_b])
    def produce_data_b():
        print("Producing data for data_b")
        return {"key": "value_b"}

    task_a = produce_data_a()
    task_b = produce_data_b()

    task_a >> task_b

producer_dag()

@dag(start_date=datetime(2023, 10, 1), schedule_interval=None, catchup=False, tags=["producer_b_dag"])
def producer_b_dag():
    @task(outlets=[data_c])
    def produceb_data_c():
        print("Producing data for data_c")
        return {"key": "value_c"}

    @task(outlets=[data_d])
    def produceb_data_d():
        print("Producing data for data_d")
        return {"key": "value_d"}

    task_c = produceb_data_c()
    task_d = produceb_data_d()

    task_c >> task_d

producer_b_dag()