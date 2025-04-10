import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.edgemodifier import Label
from datetime import datetime
from airflow.utils.task_group import TaskGroup


with DAG(
    dag_id="DataQualityOrchestration",
    schedule=None,
    start_date=None,  # Updated start date
    catchup=False,
    tags=["DataQuality"],
) as dag:

    with TaskGroup("FCT_LOAD") as load_group:
        BATCH_ACT_VAL = BashOperator(
            task_id="BATCH_ACT_VAL",
            bash_command="set -e; docker exec spark-master /bin/bash -c '/mnt/apps/run_dm_spark.sh --batchname BATCH_ACT_VAL'",
            dag=dag,
        )
        BATCH_PEOPLE = BashOperator(
        task_id="BATCH_PEOPLE",
        bash_command="set -e; docker exec spark-master /bin/bash -c '/mnt/apps/run_dm_spark.sh --batchname BATCH_PEOPLE'",
        dag=dag,
        )

    with TaskGroup("FCT_DQC") as dqc_group:
        DQC_BATCH_ACT_VAL = BashOperator(
            task_id="DQC_BATCH_ACT_VAL",
            bash_command="set -e; docker exec spark-master /bin/bash -c '/mnt/apps/run_dqc_spark.sh --batchname BATCH_ACT_VAL'",
            dag=dag,
        )

    BATCH_RTNPF = BashOperator(
        task_id="BATCH_RTNPF",
        bash_command="set -e; docker exec spark-master /bin/bash -c '/mnt/apps/run_dm_spark.sh --batchname BATCH_RTNPF'",
        dag=dag,
    )

    DQC_BATCH_RTNPF = BashOperator(
        task_id="DQC_BATCH_RTNPF",
        bash_command="set -e; docker exec spark-master /bin/bash -c '/mnt/apps/run_dqc_spark.sh --batchname BATCH_RTNPF'",
        dag=dag,
    )

    END = PythonOperator(
        task_id="END",
        python_callable=lambda: print("Jobs completed successfully"),
        depends_on_past=True,
        dag=dag,
    )

    [BATCH_RTNPF] >> DQC_BATCH_RTNPF >> END 
    load_group >> Label("There are 3 tables involved here") >> DQC_BATCH_ACT_VAL >> END