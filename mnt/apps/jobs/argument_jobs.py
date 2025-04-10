import sys
import logging

if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') # Configure logging
    logging.info(f"Running script with batchdate from python logging info before if len >>> {len(sys.argv)} > stringfy > {sys.argv[1]}")

    if len(sys.argv) != 2:
        logging.info("Usage: python argument_jobs.py <date>", file=sys.stderr)
        sys.exit(2)  # Exit with code 2 for incorrect arguments
    
    if len(sys.argv) > 1:
        batchdate = sys.argv[1]
        logging.info(f"if statement Running script with batchdate from python logging info arguments: {batchdate}")

        # Your Python script logic here using the batchdate
        # ... (e.g., process data for the given date)
        # Example:
        try:
            from datetime import datetime
            dt = datetime.strptime(batchdate, "%Y-%m-%d") # Example date format, adjust if needed
            print(f"Parsed date: {dt}")
        except ValueError:
            print(f"Invalid date format: {batchdate}. Please use YYYY-MM-DD.")
            exit(1) # Indicate error to subprocess
        # ...

    else:
        logging.info(f"else condition why >>> {len(sys.argv)} > stringfy > {sys.argv[1]}")
        print("Error: batchdate parameter is required.")
        exit(1)  # Important: Exit with a non-zero code to signal an error