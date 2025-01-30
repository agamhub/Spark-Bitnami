import sys
from random import random
from operator import add
import logging
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("-a","--sparkname",type=str,help="Spark name",required=True)
parser.add_argument("-b","--jobname",type=str,help="Job name",required=True)
parser.add_argument("-c","--partitions",type=str,default=2,help="Job name",required=False)
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

def main():
    try:
        """
        Usage: pi [partitions]
        """
        spark = SparkSession\
            .builder\
            .appName(f"{args.sparkname}")\
            .getOrCreate()
    
        partitions = args.partitions
        n = 100000 * partitions
    
        def f(_: int) -> float:
            x = random() * 2 - 1
            y = random() * 2 - 1
            return 1 if x ** 2 + y ** 2 <= 1 else 0
            
        count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
        print("Pi is roughly %f" % (4.0 * count / n))
    
        spark.stop()
        logger.info("Pi calculation completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    logger.info(
        f"""
            List Of Parameters
            -----------------------------------------------
            SparkName Mandatory = {args.sparkname}
            JobNMame Mandatory = {args.jobname}
        """
    )
    main()
    logging.shutdown()