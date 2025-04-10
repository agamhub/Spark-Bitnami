from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def process_data(logical_date, **context):
    """
    Processes data based on the logical date.
    """
    date_str = datetime.fromisoformat(logical_date).strftime('%Y%m%d')  # Format as YYYY-MM-DD
    batchname = context['dag_run'].conf.get('batchname')
    jobname = context['dag_run'].conf.get('jobname')

    # Assuming you have a database or data source
    # that is partitioned by date
    #data = get_data_from_source(date_str)  # Replace with your data retrieval logic

    print(f"Processing batchname > {batchname} > jobname > {jobname} for {date_str}")

    # Now you can use the logical_date to filter data, etc.
    # ... your data processing logic ...

with DAG(
    dag_id='parameterDAG',
    schedule_interval=None,  # Manual triggering
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        op_kwargs={'logical_date': '{{ logical_date }}'},  # Pass it as a template
        dag=dag,
    )

    END = PythonOperator(
        task_id='END',
        python_callable= lambda: print("Job Successfully ended"),
        dag=dag,
    )

process_task >> END