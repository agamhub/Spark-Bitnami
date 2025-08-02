from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

@dag(start_date=datetime(2023, 10, 1), schedule_interval=None, catchup=False, tags=["TriggerDAG"])
def TriggerDAG():
    @task
    def create_parameters():
        # Imagine this task generates some parameters
        params = {
            "hostname": "your_remote_host",
            "username": "your_username",
            "command": "ls -l /tmp"
        }
        return params

    generate_params = create_parameters()

    trigger_ssh_dag = TriggerDagRunOperator(
        task_id="trigger_ssh_dag",
        trigger_dag_id="ssh_to_host_with_paramiko",  # Matches the dag_id of the second DAG
        conf=generate_params,
        wait_for_completion=False,  # You can set this to True if you want to wait
        reset_dag_run=True,      # Optional: Reset the DAG run of the triggered DAG
    )

    generate_params >> trigger_ssh_dag

TriggerDAG()