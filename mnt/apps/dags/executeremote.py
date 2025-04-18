from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os
import paramiko  # Import the paramiko library

# Define the path to your configuration file
CONFIG_FILE_PATH = os.path.join("./jobs", "host.json")

# Load the configuration
try:
    with open(CONFIG_FILE_PATH, 'r') as f:
        remote_config = json.load(f)
        REMOTE_HOST = remote_config.get('remote_host')
        REMOTE_USER = remote_config.get('remote_user')
        SSH_KEY_PATH = remote_config.get('ssh_key_path')
        REMOTE_SCRIPT_PATH = remote_config.get('remote_script_path')
except FileNotFoundError as e:
    raise FileNotFoundError(f"Configuration file not found: {e}")
except json.JSONDecodeError as e:
    raise ValueError(f"Error decoding JSON: {e}")
except KeyError as e:
    raise KeyError(f"Missing key in config: {e}")

def execute_remote_script_with_paramiko(**kwargs):
    """Executes a remote script using paramiko."""
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # For demonstration, consider a more secure policy

    try:
        private_key = paramiko.RSAKey.from_private_key_file(SSH_KEY_PATH)
        ssh_client.connect(hostname=REMOTE_HOST, username=REMOTE_USER, pkey=private_key)

        command = f"bash {REMOTE_SCRIPT_PATH}"
        stdin, stdout, stderr = ssh_client.exec_command(command)

        # Print the output (you might want to log this in a production environment)
        for line in stdout:
            print(f"STDOUT: {line.strip()}")
        for line in stderr:
            print(f"STDERR: {line.strip()}")

        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            raise Exception(f"Remote command failed with exit code: {exit_status}")

    except Exception as e:
        raise Exception(f"Error executing remote command: {e}")
    finally:
        ssh_client.close()

with DAG(
    dag_id='ssh_to_host_with_paramiko',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    execute_remote_script = PythonOperator(
        task_id='execute_remote_script_on_host',
        python_callable=execute_remote_script_with_paramiko,
        # You can pass configuration as op_kwargs if needed
        # op_kwargs={'remote_host': REMOTE_HOST, ...}
    )

execute_remote_script