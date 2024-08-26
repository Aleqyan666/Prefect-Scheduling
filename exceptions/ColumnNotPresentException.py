class ColumnNotPresentException(Exception):
    """Custom exception for when a column is not present in a DataFrame."""

    def __init__(self, column_name, df_name):
        """
        Initialize the exception with a column name and DataFrame name.

        Args:
            column_name (str): The name of the column that was not found.
            df_name (str): The name of the DataFrame in which the column was not found.
        """
        self.message = f"Column '{column_name}' was not found in the DataFrame '{df_name}'."
        super().__init__(self.message)
