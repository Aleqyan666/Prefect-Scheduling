"""
This script demonstrates the usage of Prefect 2.0 to create artifacts from CSV data.
The main goal is to read CSV files, convert their content into a Markdown table, 
and log this information using Prefect's artifact creation functionality.

### Components:

1. **read_csv_file(task)**:
    - Reads a CSV file into a pandas DataFrame.
    - Converts the DataFrame into a Markdown table format.
    - Logs the Markdown table as an artifact using `create_markdown_artifact()`.

2. **process_csv_files(flow)**:
    - This is the main flow that orchestrates the reading of two CSV files.
    - The data from both files are combined into a single DataFrame.
    - The combined DataFrame is then converted to a Markdown table, 
      and the content of one of the files is also logged as a Markdown artifact.

### Functions:

- **read_csv_file(file_path: str) -> pd.DataFrame**:
    - Reads a CSV file from the provided `file_path`.
    - Converts the data into Markdown format and logs it as an artifact.
    - Returns the DataFrame created from the CSV file.

- **process_csv_files() -> pd.DataFrame**:
    - Executes the flow, reading two CSV files (`games.csv` and `sample.csv`), 
      concatenating the DataFrames, and logging the content of one of the files as an artifact.
    - Returns the combined DataFrame.
    
### Usage:

- To use this script, provide valid paths for the CSV files inside the `process_csv_files` flow.
- The resulting artifacts (Markdown tables) can be viewed in the Prefect UI after running the flow.
"""

import pandas as pd
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

@task
def read_csv_file(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    
    markdown_table = df.to_markdown(index=False)
    
    create_markdown_artifact(
        markdown=markdown_table
    )
    
    return df

@flow(name="Artifact testing")
def process_csv_files():
    df1 = read_csv_file("data/games.csv")
    df2 = read_csv_file("data/sample.csv")

    combined_df = pd.concat([df1, df2])
    
    markdown = df2.to_markdown(index=False)
    create_markdown_artifact(
        markdown=markdown
    )

    return combined_df

if __name__ == "__main__":
    process_csv_files()
