import sys
import os
from random import random
from operator import add
from tabulate import tabulate
import logging
import argparse
import pandas as pd
from threading import Thread
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

if __name__ == "__main__":
    path = "/mnt/apps/gcs/Config/DataQuality_Config.csv"
    LoadPath = "/mnt/apps/gcs/Config/master_job.csv"
    parquetOutput = "/mnt/apps/gcs/data-movement/Parquet/"

    df = pandas_read_csv(path, sep="|")
    df = df.query(f"BatchName == '{args.batchname}'")
    logger.info(tabulate(df.head(), headers='keys', tablefmt='pretty'))
    df_job = pandas_read_csv(LoadPath, sep="|")
    df_job = df_job.query(f"BatchName == '{args.batchname}'")
    logger.info(tabulate(df_job.head(), headers='keys', tablefmt='pretty'))
    
    sparkDqc = SparkSession. \
            builder. \
            appName(f"{args.batchname}_DQC").getOrCreate()
    
    for row in df_job.itertuples():    
        df_output = loadTable(path=parquetOutput + '/' + row.JobName + '/part*', tableName=row.JobName)
        
    print(ListTable())

    df = sparkDqc.sql("SELECT COUNT(1) A FROM ACMVPF")
    print(df.show())

    sparkDqc.stop()