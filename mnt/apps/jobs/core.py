import subprocess

hostname = "172.17.0.1"
username = "agam"
password = "your_test_password"  # NEVER DO THIS IN PRODUCTION
remote_script_path = "/mnt/e/Repo/Spark-Bitnami/airflow-data/sync.sh"
ssh_key_path = "/mnt/apps/jobs/ssh"  # Optional

try:
    command = [
        "ssh",
        "-i",
        ssh_key_path,
        f"{username}@{hostname}",
        f"bash {remote_script_path}"
    ]

    result = subprocess.run(command, capture_output=True, text=True, check=True)

    print("Script executed successfully on remote server:")
    print(result.stdout)

except subprocess.CalledProcessError as e:
    print(f"Error executing script on remote server:")
    print(e.stderr)
except Exception as e:
    print(f"An unexpected error occurred: {e}")