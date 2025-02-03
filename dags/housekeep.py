from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    dag_id='housekeeping_job',
    default_args=default_args,
    schedule_interval=None,  # You can set a schedule or trigger manually
    catchup=False, # Important: Set to False to avoid backfilling
    tags=['housekeeping'],
)

# Define the Python function to be executed
def execute_python_job(**kwargs):
    # Get the batchdate parameter from the Airflow context
    batchdate = kwargs['dag_run'].conf.get('batchdate')  # Get from DAG Run conf

    if batchdate is None:
        raise ValueError("batchdate parameter is missing in DAG run configuration.")

    # Construct the path to your Python script
    script_path = os.path.join("./jobs", "argument_jobs.py")  # Replace with your script name

    # Execute the Python script with the batchdate parameter
    try:
      # Using subprocess for more control (better for potential errors)
      import subprocess
      result = subprocess.run(["python", script_path, batchdate], capture_output=True, text=True, check=True)
      print(result.stdout)  # Print the captured standard output
      print(result.stderr)  # Print the captured standard error (if any)
      # Alternative using os.system (simpler, but less control over error handling):
      # os.system(f"python {script_path} {batchdate}")

      print(f"Successfully executed here after py {script_path} with batchdate: {batchdate}")
    except subprocess.CalledProcessError as e:
      print(f"Error executing script: {e}")
      raise  # Re-raise the exception to mark the task as failed in Airflow

    except FileNotFoundError:
      print(f"Error: Script not found at {script_path}")
      raise  # Re-raise to fail the Airflow task

    except Exception as e:
      print(f"An unexpected error occurred: {e}")
      raise


# Define the PythonOperator
execute_job_task = PythonOperator(
    task_id='execute_job',
    python_callable=execute_python_job,
    provide_context=True,  # Important: To access DAG run context
    dag=dag,
)

# No dependencies needed in this simple example.
# If you had other tasks, you would define dependencies here, e.g.:
# execute_job_task >> another_task