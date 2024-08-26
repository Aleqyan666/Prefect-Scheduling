import pandas as pd
from os.path import basename
import os
import logging
import datetime
from datetime import timedelta
import time
from logger import bcolors, CustomFormatter
from prefect import flow, task
from exceptions import FileNotFoundException, OutOfBoundsException, ColumnNotPresentException

# Functions in this file:
# - read_csv(file_path: str) -> pd.DataFrame:
# - change_value(file_path: str, column_name: str, new_value: str)
# - inspect_data(file_path: str)
# - preview_data(file_path: str, num_rows: int)
# - merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, on_column: str, how: str = 'inner') -> pd.DataFrame:
# - sort_dataframe(df: pd.DataFrame, columns: list, ascending: bool = True) -> pd.DataFrame:


path = 'data/games.csv'
path2 = 'data/sample.csv'

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


@task(persist_result=True) # Persists the task result for later retrieval
def read_csv(file_path: str)-> pd.DataFrame:
    """
    Read a CSV file into a DataFrame if it exists.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        DataFrame: The data from the CSV, or None if the file doesn't exist.

    Raises:
        FileNotFoundException: If the file does not exist.
    """
    if os.path.exists(file_path):
        logger.info(f"The file '{file_path}' exists.")
        df = pd.read_csv(file_path)
        logger.info(f"The file '{file_path}'has been successfully read.")
        logger.info(f"Shape of '{file_path}'is: {df.shape}.")
        return df

    else:
        logger.critical(f"The file '{file_path}' doesn't exist.")
        raise FileNotFoundException(file_path)


def change_value(file_path: str, column_name: str, new_value: str):
    """
    Update all values in a specific column of a CSV file.

    Reads the CSV, replaces all values in the specified column with a new value,
    and saves the changes back to the file.

    Args:
        file_path (str): Path to the CSV file.
        column_name (str): Column to update.
        new_value (str): New value to set for all rows in the column.
        """
    df = read_csv(file_path)
    logger.info(f"Setting '{new_value}' as a new value for the column {column_name}.")
    df[column_name] = new_value
    logger.info("Values successfully changed")
    df.to_csv(file_path, index=False)
    logger.info(f"Changes successfully applied to '{file_path}'")


def inspect_data(file_path: str):
    """
    Load and inspect a CSV file.

    Reads a CSV file into a pandas DataFrame, prints the DataFrame to the console,
    and displays summary statistics of the data.

    Args:   
        file_path (str): Path to the CSV file to be inspected.

    Raises: 
        FileNotFoundError: If the file does not exist.
    """
    df = read_csv(file_path)
    print(df)
    logger.info("DataFrame contents printed to console.")
    print(df.describe())
    logger.info("Summary statistics of DataFrame printed to console.")


def preview_data(file_path: str, num_rows: int):
    """
    Preview the first few rows of a DataFrame from a CSV file.

    Args:
        file_path (str): Path to the CSV file.
        num_rows (int): Number of rows to preview from the DataFrame.

    Raises: 
        OutOfBoundsException: Raised if the requested number of rows exceeds the DataFrame's size.
    """
    if num_rows > len(df):
        df = read_csv(file_path)
        print(df.head(num_rows))
        logger.info(f"First {num_rows} rows of the DataFrame printed to console.")

    else:
        logger.exception(f"Number of requested rows ({num_rows}) exceeds the DataFrame size. Displaying all available rows.")
        raise OutOfBoundsException(num_rows)
    

def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, on_column: str, how: str = 'inner') -> pd.DataFrame:
    """
    Merge two DataFrames on a specific column with a specified join method.

    Args:
        df1 (pd.DataFrame): The first DataFrame to merge.
        df2 (pd.DataFrame): The second DataFrame to merge.
        on_column (str): The column name on which to merge the DataFrames.
        how (str): Type of join to perform. Options are 'left', 'right', 'outer', 'inner'. Default is 'inner'.

    Returns:
        pd.DataFrame: A DataFrame resulting from merging `df1` and `df2`.

    Raises:
        ColumnNotPresentException: If the `on_column` is not present in either DataFrame.
    """
    if on_column not in df1.columns:
        logger.critical(f"Column '{on_column}' not found in the first DataFrame.")
        raise ColumnNotPresentException(on_column, df1)   
       
    if on_column not in df2.columns:
        logger.critical(f"Column '{on_column}' not found in the second DataFrame.")
        raise ColumnNotPresentException(on_column, df2)   
    
    merged_df = pd.merge(df1, df2, on=on_column, how=how)
    logger.info(f"DataFrames merged successfully on column '{on_column}' with '{how}' join.")
    return merged_df


def sort_dataframe(df: pd.DataFrame, columns: list, ascending: bool = True) -> pd.DataFrame:
    """
    Sort a DataFrame by one or more columns.

    Args:
        df (pd.DataFrame): The DataFrame to be sorted.
        columns (list): List of column names to sort by.
        ascending (bool): Whether to sort in ascending order. Default is True.

    Returns:
        pd.DataFrame: The sorted DataFrame.

    Raises:
        ColumnNotPresentException: If the `on_column` is not present in either DataFrame.
    """
    # Checking if all the columns are in the DataFrame
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        logger.critical(f"Columns {missing_columns} are not present in the DataFrame.")
        raise ColumnNotPresentException(missing_columns, df)
    # Do the sorting if all the columns are present in the DataFrame
    else:
        logger.info(f"DataFrame sorted by columns {columns} in {'ascending' if ascending else 'descending'} order.")
        return df.sort_values(by=columns, ascending=ascending)





@flow(name="CSV Manipulation Flow", log_prints=True) # Logs print statements within the flow.
def csv_manipulation_flow():
    result_one = read_csv(path)
    time.sleep(60) # Setting a 60seconds interval between these 2 tasks
    result_two = second_task()

    print(result_one)
    print(result_two)


@task(persist_result=True) # Persists the task result for later retrieval
def second_task():
    logger.warning('Second task finished')

# Setup logger to log both to console and file
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)  # Capture all log levels

# StreamHandler for console output
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)  # Ensuring that all levels are being captured
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

# FileHandler for logging to a file
current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f"logs/{os.path.basename(__file__)}_{current_time}.log"
# log_file = f"logs/log_{current_time}.log" before was "log_2024-08-20_16-14-11.log"

# Ensuring that the logs directory exists
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# FileHandler for writing logs to a file
fh = logging.FileHandler(log_file)
fh.setLevel(logging.DEBUG)  # Ensuring that all levels are being captured
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s - line: %(lineno)d')
fh.setFormatter(file_formatter)
logger.addHandler(fh)


if __name__=='__main__':

    # read_csv('data/games.csv')
    csv_manipulation_flow()

    for handler in logger.handlers:
        handler.flush()
        handler.close()