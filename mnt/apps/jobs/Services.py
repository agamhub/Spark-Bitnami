import pandas as pd

def pandas_read_csv(file_path, **options):
    """
        Read small volume of data only using read.csv
        Args:
            **Options ----> Any
    """
    try:
        df = pd.read_csv(file_path, **options)
        return df
    except FileNotFoundError:
        print(f"Error: File not found at path: {file_path}")
        return None
    except Exception as e:  # Catch other potential exceptions (e.g., parsing errors)
        print(f"An error occurred while reading the CSV: {e}")
        return None