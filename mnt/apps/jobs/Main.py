import sys
import os
from random import random
from operator import add
import logging
import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("-a","--sparkname",type=str,help="Spark name",required=True)
parser.add_argument("-b","--jobname",type=str,help="Job name",required=True)
parser.add_argument("-c","--partitions",type=int,default=3,help="Partitions",required=False)
args = parser.parse_args()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler(f'./logs/{args.jobname}.log')
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
    
def pandas_read_csv(file_path,**options):
    """
        Read small volume of data only using read.csv
        Args:
            **Options ----> Any
    """
    try:
        df = pd.read_csv(file_path,**options)
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
    for field in kwargs["dtypes"]:
        colName = field.name
        dType = str(field.dataType)
        if "decimal" in dType.lower():
            #print(colName)
            df_cleaned = df_contents.withColumn(
                f"{colName}_cleaned",
                regexp_replace(col(colName), "[^0-9.]", "")
            )
            df_empty = df_cleaned.filter((col(f"{colName}_cleaned")).isNull()) # is null due to schema defined as decimal if we use inferSchema its a string
            empty_count = df_empty.count()

            if empty_count > 0:
                is_valid = True
                sample_size = min(100, empty_count)
                invalid_rows = df_empty.select(colName).take(sample_size)
                error_msg = (f"Invalid {colName} values (containing only non-numeric characters) in {FullPathSchema}. Total count: {empty_count}")
                errors.append(error_msg)
            df_contents = df_contents.drop(f"{colName}_cleaned") 
    msg = "\n".join(errors) if errors else "Data Quality Check Passed."
    return is_valid, msg, df_contents

def writeOptions(df, dataMovement, **kwargs):
    base_options = {  # Keep this separate
        "header": "true",
        "delimiter": "|",
        "quote": '"'
    }
    base_options.update(kwargs)
    
    df.repartition(30).write.format("csv").mode("overwrite").options(**base_options).save(dataMovement)

def writeToParquet(df, path, schema):
    df = df.repartition(30)
    df.write.parquet(path, mode="overwrite")
    df_read_parquet = spark.read.parquet(path, schema=schema)

    return df_read_parquet

if __name__ == "__main__":
    try:
        path = "/mnt/apps/gcs/Config/master_job.csv"
        pathSchema = "/mnt/apps/gcs/Schema/"
        outputFile = "/mnt/apps/gcs/data-movement/"
        parquetOutput = "/mnt/apps/gcs/data-movement/Parquet/"

        df = pandas_read_csv(path,sep="|")
        #df = df.query("JobName == 'PAT'") #for filter debugging purposes only

        for row in df.itertuples():  # Collects all data to the driver - NOT recommended for large datasets
            filePath = row.SourceDirectory + '/' + row.FileName + '*' + '.' + row.FileType
            FullPathSchema = pathSchema + row.FileName + '.' + row.FileType
            dataMovement = outputFile + row.JobName
            dataMovementParquet = parquetOutput + row.JobName
            spark = SparkSession. \
                    builder. \
                    appName(f"{row.JobName}").getOrCreate()
            df_dtype = construct_sql_schema(path=FullPathSchema, sep="|")
            df = spark_read_csv_from_os(spark, filePath, schema=df_dtype, quote='"', sep=row.Delimiter)
            result, dqc_msg, df_final = validateDecimal(dtypes=df_dtype, df_contents=df)
            if result:
                #parquet_df = writeToParquet(df, dataMovementParquet, schema=df_dtype)
                writeOptions(df_final, dataMovement, compression="gzip")
                logger.info(dqc_msg)
            else:
                #parquet_df = writeToParquet(df, dataMovementParquet, schema=df_dtype)
                writeOptions(df_final, dataMovement, compression="gzip")
                logger.info(dqc_msg)
    
        logger.info(
            f"""
                List Of Parameters
                -----------------------------------------------
                SparkName Mandatory = {args.sparkname}
                JobNMame Mandatory = {args.jobname}
            """
        )

        spark.stop()
        logging.shutdown()
    except Exception as e:
        raise ValueError("Error Occurred main function")