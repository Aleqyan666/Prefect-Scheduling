from prefect import flow, task
import pandas as pd
import os
import logging
from logger import bcolors, CustomFormatter


logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

# Reading and processing dataset 1
@task
def read_dataset1(file_path: str) -> pd.DataFrame: 
    df = pd.read_csv(file_path)
    logger.info(f"Dataset 1 read from {file_path}")
    return df

@task
def process_dataset1(df: pd.DataFrame) -> pd.DataFrame:
    df_processed = df[df['Year'] > 50]
    logger.info("Dataset 1 processed")
    logger.info(f"Processed Dataset 1 head:\n{df_processed.head()}")
    return df_processed

@task
def print_dataset1(df: pd.DataFrame):
    logger.info("Final Dataset 1:")
    logger.info(f"{df.head()}")

# Reading and processing dataset 2
@task
def read_dataset2(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    logger.info(f"Dataset 2 read from {file_path}")
    return df

@task
def process_dataset2(df: pd.DataFrame) -> pd.DataFrame:
    df['Year'] = df['Year'] * 1.5
    logger.info("Dataset 2 processed")
    logger.info(f"Processed Dataset 2 head:\n{df.head()}")
    return df

@task
def print_dataset2(df: pd.DataFrame):
    logger.info("Final Dataset 2:")
    logger.info(f"{df.head()}")

# Reading and processing dataset 3
@task
def read_dataset3(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    logger.info(f"Dataset 3 read from {file_path}")
    return df

@task
def process_dataset3(df: pd.DataFrame) -> pd.DataFrame:
    df['Year'].fillna(value=0, inplace=True)
    logger.info("Dataset 3 processed")
    logger.info(f"Processed Dataset 3 head:\n{df.head()}")
    return df

@task
def print_dataset3(df: pd.DataFrame):
    logger.info("Final Dataset 3:")
    logger.info(f"{df.head()}")

# Setup logger to log both to console and file
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)  # Capture all log levels

# StreamHandler for console output
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)  # Ensuring that all levels are being captured
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


# Flow for parallel execution
@flow
def parallel_processing():
    dataset1_path = "data/games.csv"
    dataset2_path = "data/games.csv"
    dataset3_path = "data/games.csv"

    # Runing the tasks for each dataset in parallel
    df1_future = read_dataset1.submit(dataset1_path)
    processed_df1_future = process_dataset1.submit(df1_future)
    print_dataset1.submit(processed_df1_future)

    df2_future = read_dataset2.submit(dataset2_path)
    processed_df2_future = process_dataset2.submit(df2_future)
    print_dataset2.submit(processed_df2_future)

    df3_future = read_dataset3.submit(dataset3_path)
    processed_df3_future = process_dataset3.submit(df3_future)
    print_dataset3.submit(processed_df3_future)

# Execute the flow
if __name__ == "__main__":
    parallel_processing()
