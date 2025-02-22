#!/bin/bash

# Prepare arguments for Main.py
MAIN_ARGS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --batchname)
            BATCHNAME="$2"
            MAIN_ARGS+="--batchname $BATCHNAME " # Add to Main.py args
            shift 2
            ;;
       *)
            # Handle other arguments if needed for Main.py
            MAIN_ARGS+="$1 " # Add other arguments for Main.py
            shift 1
            ;;
    esac
done

# Check if both sparkname and jobname are provided
if [ -z "$BATCHNAME" ]; then
  echo "Error: argument --batchname is required."
  echo "Usage: ./run_dm_spark.sh --batchname <batchname>"
  exit 1
fi

# Set variables (LOGDIR remains the same)
LOGDIR="./logs"
LOGFILE="$LOGDIR/$BATCHNAME.log"

# Create log directory if it doesn't exist
mkdir -p "$LOGDIR"

echo $MAIN_ARGS

# Function to handle errors
handle_error() {
  echo "Error running Spark job:"
  cat "$LOGFILE"  # Display the log file content
  exit 1
}

# Run spark-submit and redirect output to log file
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name "$SPARKNAME" \
  --driver-memory 2g \
  --num-executors 2 \
  --executor-memory 1024M \
  --conf spark.cores.max=3 \
  --conf "spark.sql.shuffle.partitions=24" \
  /mnt/apps/jobs/SparkDataMovement.py $MAIN_ARGS 2>&1 | tee "$LOGFILE"

# Check the exit status of spark-submit
if [[ $? -ne 0 ]]; then
  handle_error
fi

echo "$(date) - BASH - Spark job '$BATCHNAME' completed" | tee -a "$LOGFILE"
echo "Spark job '$BATCHNAME' submitted. Logs are in $LOGFILE"