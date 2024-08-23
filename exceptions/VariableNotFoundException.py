class VariableNotFoundError(Exception):
    """Custom exception for when a Prefect variable is not found."""
    
    def __init__(self, var_name):
        self.message = f"Variable '{var_name}' was not found in Prefect/Variables."
        super().__init__(self.message) 