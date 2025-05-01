import pandas as pd
import requests
import sys
from pyspark.sql import Row

def pandas_read_csv(file_path, **options):
    """
        Read small volume of data only using read.csv
        Args:
            **Options ----> Any
    """
    try:
        df = pd.read_csv(file_path, **options)
        return df
    except FileNotFoundError:
        print(f"Error: File not found at path: {file_path}")
        return None
    except Exception as e:  # Catch other potential exceptions (e.g., parsing errors)
        print(f"An error occurred while reading the CSV: {e}")
        return None
    
def get_spark_app_id(app_name):
    ports=[4040, 4041, 4042, 4043, 4044, 4045]

    for port in ports:
        master_url = f"http://localhost:{port}"
        try:
            response = requests.get(f"{master_url}/api/v1/applications", timeout=10)
            response.raise_for_status()
            applications = response.json()

            for apps in applications:
                if apps["name"] == app_name:
                    #print(apps["attempts"][0]["completed"])
                    return 1
        except requests.exceptions.RequestException as e:
            pass
    return 0

def dict_to_row(data_list):
    """Converts a dictionary to a Spark Row."""
    data_dict = data_list[0]
    return Row(**data_dict)

def create_dataframe_from_dict(spark, data_dict):
    """Creates a Spark DataFrame from a dictionary."""
    row = dict_to_row(data_dict)
    return spark.createDataFrame([row])

def print_different_headers(csv_headers, schema_headers):
    """
    Prints the headers that are in a different order between two lists.

    Args:
        csv_headers (list): A list of header names from a CSV file.
        schema_headers (list): A list of header names from a schema.
    """
    msg = []
    if csv_headers == schema_headers:
        print("Headers are in the same order.")
        return "None"

    print("Headers are in a different order:")
    for i, (csv_header, schema_header) in enumerate(zip(csv_headers, schema_headers)):
        if csv_header != schema_header:
            msg.append(f"Position {i+1}: CSV has '{csv_header}', Schema has '{schema_header}'")

    return(msg)


if __name__ == "__main__":
    app_name = sys.argv[1]  # Get app_name from command-line argument
    result = get_spark_app_id(app_name)
    sys.exit(result)  # Set the exit code to the result