#!/bin/bash

# Prepare arguments for Main.py
MAIN_ARGS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sparkname)
            SPARKNAME="$2"
            MAIN_ARGS+="--sparkname $SPARKNAME " # Add to Main.py args
            shift 2
            ;;
        --jobname)
            JOBNAME="$2"
            MAIN_ARGS+="--jobname $JOBNAME "  # Add to Main.py args
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
if [ -z "$SPARKNAME" ] || [ -z "$JOBNAME" ]; then
  echo "Error: Both --sparkname and --jobname options are required."
  echo "Usage: ./run_spark.sh --sparkname <sparkname> --jobname <jobname>"
  exit 1
fi

# Set variables (LOGDIR remains the same)
LOGDIR="./logs"
LOGFILE="$LOGDIR/$JOBNAME.log"

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
  --driver-memory 1g \
  --conf spark.executor.memory=1524M \
  --conf spark.cores.max=2 \
  /mnt/apps/jobs/Main.py $MAIN_ARGS 2>&1 | tee "$LOGFILE"

# Check the exit status of spark-submit
if [[ $? -ne 0 ]]; then
  handle_error
fi

echo "$(date) - BASH - Spark job '$JOBNAME' completed" | tee -a "$LOGFILE"
echo "Spark job '$JOBNAME' submitted with name '$SPARKNAME'. Logs are in $LOGFILE"