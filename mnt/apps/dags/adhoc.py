from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain

# Define the list of all possible batch names
ALL_BATCH_NAMES = ["BATCH_PEOPLE", "BATCH_ACT_VAL", "BATCH_OTHER_1", "BATCH_OTHER_2", "BATCH_OTHER_3",
                     "BATCH_TASK_A", "BATCH_TASK_B", "BATCH_PROCESS_X", "BATCH_DATA_CLEAN", "BATCH_REPORT_GEN"]

with DAG(
    dag_id='dynamic_selective_batch_execution',
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    params={"selective_batch_names": ALL_BATCH_NAMES},
    tags=['dynamic_example'],
) as dag:
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "DAG started"',
    )

    def decide_batches_to_run(**kwargs):
        selected_batches = kwargs['dag_run'].conf.get('selective_batch_names')
        if not selected_batches:
            return ALL_BATCH_NAMES  # Run all if no parameter is provided
        elif isinstance(selected_batches, str):
            selected_batches = [batch.strip() for batch in selected_batches.split(',')]
            return selected_batches
        elif isinstance(selected_batches, list):
            return selected_batches
        else:
            return "end_task"  # Handle cases with unexpected parameter type

        tasks_to_run = []
        for batch_name in ALL_BATCH_NAMES:
            if batch_name in selected_batches:
                tasks_to_run.append(batch_name)

        if tasks_to_run:
            return tasks_to_run
        else:
            return "end_task"  # Or a task to handle no selection

    branch_task = BranchPythonOperator(
        task_id='decide_which_batches_to_run',
        python_callable=decide_batches_to_run,
    )

    process_batches_group = [
        BashOperator(
            task_id=batch_name,
            bash_command="set -e; docker exec spark-master /bin/bash -c '/mnt/apps/run_dm_spark.sh --batchname {batch_name}'".format(batch_name=batch_name),
        )
        for batch_name in ALL_BATCH_NAMES
    ]

    # Create a dictionary to easily access batch tasks by their task_id
    batch_tasks_dict = {task.task_id: task for task in process_batches_group}

    end_task = BashOperator(
        task_id='end_task',
        bash_command='echo "DAG finished"',
    )

    start_task >> branch_task

    for batch_name in ALL_BATCH_NAMES:
        branch_task >> batch_tasks_dict[batch_name] >> end_task