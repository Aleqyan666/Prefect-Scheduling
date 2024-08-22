import pandas as pd
from os.path import basename
import os
import logging
import datetime
from datetime import timedelta
import time
from logger import bcolors, CustomFormatter
from prefect import flow, task

PATH = 'data/games.csv'
PATH2 = 'data/sample.csv'

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


@task(persist_result=True) # Persists the task result for later retrieval
def read_csv(file_path: str):
    """
    Read a CSV file into a DataFrame if it exists.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        DataFrame: The data from the CSV, or None if the file doesn't exist.
    """
    
    if os.path.exists(file_path):
        logger.info(f"The file '{file_path}' exists.")
        df = pd.read_csv(file_path)
        logger.info(f"The file '{file_path}'has been successfully read.")
        logger.info(f"Shape of '{file_path}'is: {df.shape}.")
        return df

    else:
        logger.critical(f"The file '{file_path}' doesn't exist.")

@task(persist_result=True) # Persists the task result for later retrieval
def second_task():
    logger.warning('Second task finished')
    

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
    
@flow(name="Testing Param Flow")
def param_flow():

    result_one = read_csv(PATH)
    time.sleep(60)
    result_two = second_task()

    print(result_one)
    print(result_two)


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

    param_flow()

    for handler in logger.handlers:
        handler.flush()
        handler.close()