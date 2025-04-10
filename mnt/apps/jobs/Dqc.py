from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def data_quality_check(spark, file_path, checks):
    try:
        if file_path.endswith(".csv"):
            df = spark.read.csv(file_path, header=True, inferSchema=True, sep="|")
        elif file_path.endswith(".parquet"):
            df = spark.read.parquet(file_path)
        else:
            raise ValueError("Unsupported file type. Please use CSV or Parquet.")

        df.createOrReplaceTempView("my_table")  # Create a temporary view

        results = {}
        for check_description, sql_query in checks.items():
            try:
                # Replace __TABLE__ with the view name
                sql_query = sql_query.replace("__TABLE__", "my_table")  # IMPORTANT!
                result = spark.sql(sql_query).collect()[0][0]  # Use spark.sql()
                results[check_description] = result
            except Exception as e:
                results[check_description] = f"Error: {e}"
                print(f"Error during check '{check_description}': {e}")

        return results

    except Exception as e:
        print(f"Error reading file: {e}")
        raise  # Re-raise to fail the Spark job


def main():
    spark = SparkSession.builder.appName("DataQualityCheck").getOrCreate()

    file_path = "/mnt/apps/Files/fct.csv"  # Replace with the actual path

    checks = {
        "Total Record Count": "SELECT COUNT(1) FROM __TABLE__",
        "Count where a = 1": "SELECT COUNT(1) FROM __TABLE__ WHERE a = 1",
        "Count where b = 'value'": "SELECT COUNT(1) FROM __TABLE__ WHERE b = 'value'",
        "Sum of column c": "SELECT SUM(c) FROM __TABLE__",
    }

    try:
        results = data_quality_check(spark, file_path, checks)

        if results:
            print("Data Quality Check Results:")
            for check, result in results.items():
                print(f"- {check}: {result}")
    except Exception as e:
        print(f"Data quality check failed: {e}")  # This will be printed to the Spark logs.

    spark.stop()

if __name__ == "__main__":
    main()