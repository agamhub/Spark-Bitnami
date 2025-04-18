import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

def execute_python_job(**kwargs):

    # Construct the path to your Python script
    script_path = os.path.join("./jobs", "core1.py")  # Replace with your script name
    print(script_path)

    single_path = "core1.py"
    absolute_single_path = os.path.abspath(single_path)
    print(f"Absolute path for '{single_path}': {absolute_single_path}")

    # Execute the Python script with the batchdate parameter
    try:
      # Using subprocess for more control (better for potential errors)
      import subprocess
      result = subprocess.run(["python", script_path], capture_output=True, text=True, check=True)
      print(result.stdout)  # Print the captured standard output
      print(result.stderr)  # Print the captured standard error (if any)
      # Alternative using os.system (simpler, but less control over error handling):
      # os.system(f"python {script_path} {batchdate}")

      print(f"Successfully executed here after py {script_path}")
    except subprocess.CalledProcessError as e:
      print(f"Error executing script: {e}")
      raise  # Re-raise the exception to mark the task as failed in Airflow

    except FileNotFoundError:
      print(f"Error: Script not found at {script_path}")
      raise  # Re-raise to fail the Airflow task

    except Exception as e:
      print(f"An unexpected error occurred: {e}")
      raise

with DAG(
    dag_id="callsh",
    schedule=None,
    start_date=datetime(2024, 10, 28),  # Updated start date
    catchup=False,
    tags=["callsh"],
) as dag:
    
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Jobs started"),
        dag=dag,
    )   

    callsh = PythonOperator(
        task_id="callsh",
        python_callable=execute_python_job,
        dag=dag,
    )
    
start >> callsh