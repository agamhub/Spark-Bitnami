import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

with DAG(
    dag_id="spark_orchestration",
    schedule=None,
    start_date=datetime(2024, 10, 28),  # Updated start date
    catchup=False,
    tags=["spark"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Jobs started"),
        dag=dag,
    )

    def decide_spark_jobs(**kwargs):
        # Logic to determine which Spark job(s) to run
        # Example: Check a configuration value, external trigger, etc.
        run_dqc = Variable.get("run_dqc", True)  # Default to False if the variable is not set
        run_regular_job = Variable.get("run_regular_job", True) # Default to True if the variable is not set

        tasks_to_run = []
        if run_regular_job == "True":
            tasks_to_run.append("run_spark_job")
        if run_dqc == "True":
            tasks_to_run.append("run_spark_job_dqc")

        return tasks_to_run if tasks_to_run else "no_spark_jobs"  # Return list or task ID or a dummy Task ID

    branch_spark_jobs = BranchPythonOperator(
        task_id="branch_spark_jobs",
        python_callable=decide_spark_jobs,
        dag=dag,
    )

    spark_name = "shtest"
    job_name = "shtest"

    command = [
        "docker",
        "exec",
        "-d",
        "spark-master",
        "/bin/bash",
        "-c",
        f"'/mnt/apps/run_spark.sh --sparkname {spark_name} --jobname {job_name}'",
    ]

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command=" ".join(command),
        dag=dag,
    )

    run_spark_job_dqc = BashOperator(
        task_id="run_spark_job_dqc",
        bash_command="docker exec -d spark-master /bin/bash -c 'spark-submit --master spark://spark-master:7077 /mnt/apps/jobs/Dqc.py'",
        dag=dag,
    )

    no_spark_jobs = PythonOperator( # Dummy Task if no spark task chosen
        task_id="no_spark_jobs",
        python_callable=lambda: print("No spark job to run"),
        dag=dag
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
        dag=dag,
    )

    batch01 = PythonOperator(
        task_id="ETL1_BATCH01",
        python_callable=lambda: print("Jobs completed successfully"),
        dag=dag,
    )

    start >> branch_spark_jobs
    branch_spark_jobs >> [run_spark_job, run_spark_job_dqc, no_spark_jobs]
    [run_spark_job ,run_spark_job_dqc, no_spark_jobs] >> end  # Merge back to end
    [run_spark_job_dqc] >> batch01