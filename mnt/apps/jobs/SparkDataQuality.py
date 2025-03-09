import sys
import os
from datetime import datetime
from tabulate import tabulate
import logging
import argparse
import pandas as pd
import threading
from queue import Queue
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, lit
from pyspark.sql.types import *
from Services import * #import from services

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

def loadTable(**kwargs):
    pathCheck = kwargs["path"].replace("/part*","")
    if not os.path.exists(pathCheck):
        return None
    try:
        sparkDqc.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {kwargs["tableName"]}
        USING PARQUET LOCATION '{kwargs["path"]}'
        """)
        return True
    except Exception as e:
        return None
    
def ListTable():
    listCreatedTable = []
    tables = sparkDqc.sql("SHOW TABLES").collect()
    for table in tables:
       listCreatedTable.append(table.tableName)
    return listCreatedTable

def getTables(**kwargs):
    df = pandas_read_csv(kwargs["path"], sep="|")
    df = df.query(f"BatchName == '{args.batchname}' & Run == 1")
    print(tabulate(df.head(), headers='keys', tablefmt='pretty'))
    df_refTable = df.query("RefTable.notnull()")
    distinct_reftable = df_refTable[["BatchName","RefTable"]].drop_duplicates().rename(columns={"RefTable": "JobName"})
    df_job = pandas_read_csv(kwargs["LoadPath"], sep="|")
    df_job = df_job[["BatchName","JobName"]].query(f"BatchName == '{args.batchname}'")
    joined_df = pd.concat([df_job, distinct_reftable], ignore_index=True).drop_duplicates()
    print(tabulate(joined_df.head(), headers='keys', tablefmt='pretty'))
    return joined_df, df

def executeScripts(sparkDqc, **kwargs):
    start_time = datetime.now()
    try:
        if kwargs["SP"] == 1:
            with open(kwargs["Scripts"], 'r') as file:
                sql_content = file.read()
            sql_statements = sql_content.split(";")
            sql_statements = [s.strip() for s in sql_statements if s.strip()]

            for sql in sql_statements:
                sql_upper = sql.upper()
                if sql_upper.startswith("SELECT"): #Check if the string STARTS with select.
                    result_df = sparkDqc.sql(sql)
                else:
                    sparkDqc.sql(sql)
        else:
            result_df = sparkDqc.sql(kwargs["Scripts"])

        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        if duration_seconds < 60:
            duration = f"{duration_seconds:.2f} seconds"
        elif duration_seconds < 3600:
            duration = f"{duration_seconds / 60:.2f} minutes"
        else:
            duration = f"{duration_seconds / 3600:.2f} hours"

        count = result_df.collect()[0]
        kwargs["results"].append((kwargs["BatchName"], kwargs["DqcId"], start_time, end_time, duration, kwargs["Scripts"], kwargs["Run"], str(count["CNT"])))
    except Exception as e:
        end_time = datetime.now()
        kwargs["results"].append((kwargs["BatchName"], kwargs["DqcId"], start_time, end_time, 0, kwargs["Scripts"], kwargs["Run"], f"Error : {str(e)}"))

def executeScriptsParallel(sparkDqc, df):
    results = []
    threads = []

    for row in df.itertuples(index=False):
        kwargs = {
            "BatchName": row.BatchName,
            "DqcId": row.DqcId,
            "Scripts": row.Scripts,
            "Run": row.Run,
            "SP": row.SP,
            "results": results
        }
         
        thread = threading.Thread(target=executeScripts, args=(sparkDqc,), kwargs=kwargs)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    result_df = sparkDqc.createDataFrame(results, ["BatchName", "DqcId", "StartTime", "EndTime", "Duration", "Scripts", "Run", "Result"])
    return result_df

if __name__ == "__main__":
    path = "/mnt/apps/gcs/Config/DataQuality_Config.csv"
    LoadPath = "/mnt/apps/gcs/Config/master_job.csv"
    parquetOutput = "/mnt/apps/gcs/data-movement/Parquet/"

    getDf, df = getTables(path=path, LoadPath=LoadPath)
    
    sparkDqc = SparkSession. \
            builder. \
            appName(f"{args.batchname}_DQC").getOrCreate()
    
    listJob = []
    for row in getDf.itertuples():
        listJob.append(row.JobName.lower())
        loadTable(path=parquetOutput + '/' + row.JobName + '/part*', tableName=row.JobName)
    
    if all(item in ListTable() for item in listJob):
        result_df = executeScriptsParallel(sparkDqc, df)
        logger.info(result_df.show(truncate=False))
    else:
        sys.exit(1)

    sparkDqc.stop()