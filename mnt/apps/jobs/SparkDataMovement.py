import sys
import os
from random import random
from operator import add
import logging
import argparse
import pandas as pd
from tabulate import tabulate
from threading import Thread
from queue import Queue
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, lit
from pyspark.sql.types import *
from Services import *

parser = argparse.ArgumentParser()
parser.add_argument("-a","--batchname",type=str,help="Batch name",required=True)
parser.add_argument("-b","--jobname",type=str,help="Job name",required=False)
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

def spark_read_csv_from_os(spark, file_path, schema, header=True, **options):
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
    try:
        df = spark.read.csv(file_path, header=header, inferSchema=False, schema=schema, **options)
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
        "decimal": DecimalType
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
        if "decimal" in dType.lower() or "int" in dType.lower():
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
            
    if is_valid == False:        
        error_msg = "DDL Decimal/Int Data Type Structure Checks Passed."
        errors.append(error_msg)

    return is_valid, errors, df_contents, dqcId

def writeToParquet(df, path):
    try:
        #print("Write to parquet started : ",path)
        df = df.coalesce(50)
        df.write.parquet(path, mode="overwrite", compression="snappy")
        return True
    except Exception as e:  # Catch other potential exceptions (e.g., parsing errors)
        print(f"An error occurred while writing the parquet: {e}")
        raise ValueError("Error Here Write Parquet")
        return False

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

if __name__ == "__main__":
    try:
        path = "/mnt/apps/gcs/Config/master_job.csv"
        parquetOutput = "/mnt/apps/gcs/data-movement/Parquet/"
        dqcOutput = []
        q = Queue()
        workerCount = 2

        tables = LoadConfig(path)
        if not tables: #empty array should len(tables)
            logger.warning("Please enable flag to 1 if you want to re-order Data Movement")
            sys.exit(0) #exit but keep airflow success
        #print("tables here",tables)
        for table in tables:
            q.put(table)

        spark = SparkSession. \
                    builder. \
                    appName(f"{args.batchname}").getOrCreate()

        def loadTable(path):
            try:
                sc = path.split("/")[5]
                pathParquet = f"/mnt/apps/gcs/data-movement/Parquet/{sc}"
                PathSchema = f"/mnt/apps/gcs/Schema/{sc}.csv"
                df_dtype = construct_sql_schema(path=PathSchema, sep="|")
                df = spark.read.csv(path, header=True, inferSchema=False, schema=df_dtype, sep="|")
                result, dqc_msg, df_final, dqcId = validateDecimal(dtypes=df_dtype, df_contents=df)
                #print(df_final.show())
                df_count = df_final.count()
                if result:
                    print(dqc_msg)
                    dqcOutput.append({"JobName":sc, "Path":path, "dqID":dqcId, "CountRecords":df_count, "Message":dqc_msg, "Status":"Failed"})
                else:
                    writeToParquet(df_final, pathParquet)
                    print(dqc_msg)
                    dqcOutput.append({"JobName":sc, "Path":path, "dqID":dqcId, "CountRecords":df_count, "Message":dqc_msg, "Status":"Successful"})
            except Exception as e:
                raise ValueError("Error Occurred main function")

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
                JobNMame Mandatory = {args.jobname}
                List Of DQC result = {dqcOutput}
            """
        )
        spark.stop()
        logging.shutdown()
    except SyntaxError as se:
        error_message = str(se)
        logger.info(f"Error Syntax {se.text}")
        sys.exit(1)
    except Exception as e:
        logger.info(f"Unxpected error {e}")
        sys.exit(1)