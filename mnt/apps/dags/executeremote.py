from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import json
import os

# Define the path to your configuration file
CONFIG_FILE_PATH = os.path.join("./jobs", "host.json")
# Or a dedicated config folder: CONFIG_FILE_PATH = '/path/to/your/config/remote_config.json'

# Load the configuration from the JSON file
try:
    with open(CONFIG_FILE_PATH, 'r') as f:
        remote_config = json.load(f)
        REMOTE_HOST = remote_config.get('remote_host')
        REMOTE_USER = remote_config.get('remote_user')
        SSH_KEY_PATH = remote_config.get('ssh_key_path')
        REMOTE_SCRIPT_PATH = remote_config.get('remote_script_path')
except FileNotFoundError:
    raise FileNotFoundError(f"Configuration file not found at: {CONFIG_FILE_PATH}")
except json.JSONDecodeError:
    raise ValueError(f"Error decoding JSON from: {CONFIG_FILE_PATH}")
except KeyError as e:
    raise KeyError(f"Missing key in configuration file: {e}")

with DAG(
    dag_id='ssh_to_host_from_config',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    execute_remote_script = BashOperator(
        task_id='execute_remote_script_on_host',
        bash_command=(
            f'ssh -i {SSH_KEY_PATH} '
            '-o StrictHostKeyChecking=no '
            f'{REMOTE_USER}@{REMOTE_HOST} "bash {REMOTE_SCRIPT_PATH}"'
        ),
        env={
            'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
            # Potentially other environment variables needed by your script
        }
    )

execute_remote_script