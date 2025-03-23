import sys
import os
from random import random
from operator import add
import logging
import argparse
import pandas as pd
import atexit
from datetime import datetime
from tabulate import tabulate
from threading import Thread
from queue import Queue
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, lit, count, to_date, udf, explode
from pyspark.sql.types import *
from Services import *

parser = argparse.ArgumentParser()
parser.add_argument("-a","--batchname",type=str,help="Batch name",required=True)
args = parser.parse_args()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler(f'./logs/{args.batchname}.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Stream handler (console)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

# Add both handlers
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def spark_read_csv_from_os(spark, file_path, schema, **kwargs):
    """
    Reads a CSV file from the operating system into a Spark DataFrame.

    Args:
        spark: The SparkSession object.
        file_path: The path to the CSV file.  Can be a local path or a path
                   that your Spark environment can access (e.g., if you're
                   using a distributed file system like HDFS).
        header (bool, optional): Whether the CSV file has a header row. Defaults to True.
        inferSchema (bool, optional): Whether to infer the schema from the data. Defaults to True.
        **options: Additional options to pass to the Spark CSV reader.  See
                   the Spark documentation for available options like `delimiter`,
                   `quote`, `escape`, etc.

    Returns:
        A Spark DataFrame representing the CSV data, or None if there's an error.

    Raises:
       FileNotFoundError: If the file path doesn't exist.
    """
    base_options = {
        "inferSchema": "False",
        "header": "True",
        "quote": '"',
        "columnNameOfCorruptRecord": "rejected_records",
        "mode": "PERMISSIVE"
    }
    base_options.update(kwargs)

    try:
        schema = StructType(schema.fields + [StructField("rejected_records", StringType(), True)])
        df = spark.read.options(**base_options).schema(schema).csv(file_path)

        rejected_df = df.filter(col("rejected_records").isNotNull()).withColumn(
            "error_details", lit("Error DDL please check contents")).limit(100)
        rejected_df = rejected_df.drop("rejected_records")
        rejected_df. \
                coalesce(1).write.csv(
                    path=kwargs["logPath"],
                    mode="overwrite",
                    sep="|",
                    header=True #add header if needed.
                    )
        rejected_df.show()
        df = df.drop("rejected_records")

        return df
    except FileNotFoundError:
        print(f"Error: File not found at path: {file_path}")
        return None
    except Exception as e:  # Catch other potential exceptions (e.g., parsing errors)
        print(f"An error occurred while reading the CSV: {e}")
        return None
    
def construct_sql_schema(**kwargs):
    """
        Args: kwargs path and sep -->>> Any
        this function is best practice to compute large amount of data to not reading schema metadata
        recommendation : 
    """

    fields = []
    type_mapping = {
        "varchar": StringType(),
        "nvarchar": StringType(),
        "int": IntegerType(),
        "bigint": LongType(),
        "date": DateType(),
        "decimal": DecimalType,
        "numeric": DecimalType,
        "money": StringType()
    }

    df = pandas_read_csv(kwargs["path"],sep=kwargs["sep"])

    for row in df.itertuples():
        try:
            name, data_type_str = row.DataType.split("(", 1) if "(" in row.DataType else (row.DataType,"")
            name = name.strip()
            data_type_str = data_type_str[:-1].strip()
            parts = data_type_str.split(",")
            name_lower = name.lower()

            for keyword,spark_type in type_mapping.items():
                if keyword in name_lower:
                    if spark_type == DecimalType:
                        data_type = DecimalType() if not data_type_str else DecimalType(int(parts[0]),int(parts[1]))
                        fields.append(StructField(row.ColumnName, data_type, True))
                    else:
                        data_type = spark_type
                        fields.append(StructField(row.ColumnName, data_type, True))
                    break
        except Exception as e:  # Catch other potential errors
            print(f"Error processing file in construct schema {kwargs["path"]}: {e}")
            return None
    return StructType(fields)

def validateDecimal(**kwargs):
    df_contents = kwargs["df_contents"]
    is_valid = False
    errors = []
    dqcId = "DQ000001"
    for field in kwargs["dtypes"]:
        colName = field.name
        dType = str(field.dataType)
        if "decimal" in dType.lower() or "int" in dType.lower() or "numeric" in dType.lower() or "money" in dType.lower():
            if "money" in dType.lower():
                df_contents = df_contents.withColumn(f"{colName}",regexp_replace(",","."))

            #print(colName)
            df_cleaned = df_contents.withColumn(
                f"{colName}_cleaned",
                regexp_replace(col(colName), "[^0-9.]", "")
            )
            df_empty = df_cleaned.filter((col(f"{colName}_cleaned")).isNull()) # is null due -to schema defined as decimal if we use inferSchema its a string
            empty_count = df_empty.count()

            if empty_count > 0:
                is_valid = True
                error_msg = (f"Invalid {colName} values (containing only non-numeric characters). Total count: {empty_count}")
                errors.append(error_msg)
            df_contents = df_contents.drop(f"{colName}_cleaned") 

        elif "date" in dType.lower():
            #logger.warning(colName)
            date_format = kwargs.get("date_format", "%Y-%m-%d") #default format.

            def parse_date(date_string, format_string):
                try:
                    if isinstance(date_string, str): #Check if the data is a string.
                        if date_string:
                            return datetime.strptime(date_string, format_string).date()
                        else:
                            return None
                    else: #if the data is not a string, return None.
                        return None
                except ValueError:
                    return None

            parse_date_udf = udf(parse_date, DateType())

            df_date_check = df_contents.withColumn(
                f"{colName}_date_check",
                parse_date_udf(col(colName), lit(date_format))
            )

            #logger.info("ini harusnya :")
            df_invalid_dates = df_date_check.filter(col(f"{colName}_date_check").isNull() & col(colName).isNull())
            #df_invalid_dates.show()
            invalid_date_count = df_invalid_dates.count()

            if invalid_date_count > 0:
                is_valid = True
                error_msg = (f"Invalid {colName} values (not in '{date_format}' format). Total count: {invalid_date_count}")
                errors.append(error_msg)
            df_contents = df_contents.drop(f"{colName}_date_check") 

    if is_valid == False:        
        error_msg = "DDL Decimal/Int/Date Data Type Structure Checks Passed."
        errors.append(error_msg)

    return is_valid, errors, df_contents, dqcId

def writeToParquet(df, path):
    try:
        #print("Write to parquet started : ",path)
        df = df.coalesce(30)
        df.write.parquet(path, mode="overwrite", compression="snappy")
        return True
    except Exception as e:  # Catch other potential exceptions (e.g., parsing errors)
        print(f"An error occurred while writing the parquet: {e}")
        raise ValueError("Error Here Write Parquet")
    
def writeToGz(df, dataMovement, **kwargs):
    base_options = {  # Keep this separate
        "header": "true",
        "delimiter": "|",
        "quote": '"'
    }
    base_options.update(kwargs)
    df.coalesce(30).write.format("csv").mode("overwrite").options(**base_options).save(dataMovement)

def run_task(function, q):
    while not q.empty():
        value = q.get()
        function(value)
        q.task_done()

def LoadConfig(path):
    df = pandas_read_csv(path, sep="|")
    df = df.query(f"(BatchName == '{args.batchname}') & (Flag == 1)")
    logger.info(tabulate(df.head(), headers='keys', tablefmt='pretty'))
    tables = []
    for row in df.itertuples():
        filePath = row.SourceDirectory + '/' + row.FileName + '*.' + row.FileType
        tables.append(filePath)
    return tables

# when spark UI got killed airflow gets killed as well
def exit_handler():
    try:
        pass
    except Exception as e:
        logger.warning("Python process exit")
    finally:
        os._exit(1)

if __name__ == "__main__":
    try:
        path = "/mnt/apps/gcs/Config/master_job.csv"
        parquetOutput = "/mnt/apps/gcs/data-movement/Parquet/"
        pathLogs = "/mnt/apps/gcs/logs/"
        dqcOutput = []
        dqcId = "DQ000001"
        q = Queue()
        workerCount = 2

        tables = LoadConfig(path)
        if not tables: #empty array should len(tables)
            logger.warning("Please enable flag to 1 if you want to re-order Data Movement")
            sys.exit(0) #exit but keep airflow success
        #print("tables here",tables)
        for table in tables:
            q.put(table)

        """ 
            Args : 
                spark.cleaner.referenceTracking True -- to releasing spark mem usage from container if its got killed
                spark.task.maxFailures", 1 -- for exit(1) then throws to airflow every python code error will return 1
        """
        spark = SparkSession. \
                    builder. \
                    appName(f"{args.batchname}"). \
                    config("spark.task.maxFailures", 1). \
                    config("spark.cleaner.referenceTracking", "true"). \
                    getOrCreate()

        def loadTable(path):
            try:
                sc = path.split("/")[5]
                pathParquet = f"/mnt/apps/gcs/data-movement/Parquet/{sc}"
                pathGz = f"/mnt/apps/gcs/data-movement/{sc}"
                PathSchema = f"/mnt/apps/gcs/Schema/{sc}.csv"
                Logs = pathLogs + f"{sc}"
                df_dtype = construct_sql_schema(path=PathSchema, sep="|")
                df = spark_read_csv_from_os(spark, path, schema=df_dtype, sep="|", logPath=Logs)
                result, dqc_msg, df_final, dqcId = validateDecimal(dtypes=df_dtype, df_contents=df)
                if result:
                    print(dqc_msg)
                    dqcOutput.append({"JobName":sc, "Path":path, "dqID":dqcId, "CountRecords":-1, "Message":dqc_msg, "Status":"Failed"})
                    atexit.register(exit_handler)
                else:
                    df_count = df_final.count()
                    writeToParquet(df_final, pathParquet)
                    #writeToGz(df_final, pathGz, compression="gzip")
                    print(dqc_msg)
                    dqcOutput.append({"JobName":sc, "Path":path, "dqID":dqcId, "CountRecords":df_count, "Message":dqc_msg, "Status":"Successful"})
            except Exception as e:
                #raise ValueError("Error Occurred main function")
                logger.warning(f"Spark Session got killed or others issue encountered !!! {e}")
                atexit.register(exit_handler)

        for i in range(workerCount):
            t=Thread(target=run_task, args=(loadTable, q))
            t.daemon = True
            t.start()

        print("running load")
        q.join()
        print("running completed")

        logger.info(
            f"""
                List Of Parameters
                -----------------------------------------------
                SparkName Mandatory = {args.batchname}
                List Of DQC result = {dqcOutput}
            """
        )

        df_dqc = create_dataframe_from_dict(spark, dqcOutput)
        exploded_df = df_dqc.select("JobName", "Path", "dqID", "CountRecords", explode("Message").alias("Message"), "Status")
        exploded_df.show(truncate=False)

        spark.stop()
        logging.shutdown()
    except SyntaxError as se:
        logger.info(f"Error Syntax {se.text}")
        sys.exit(1)
    except Exception as e:
        sys.exit(1)