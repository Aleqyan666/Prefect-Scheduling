class VariableNotFoundError(Exception):
    """Custom exception for when a file is not found."""
    
    def __init__(self, file_path):
        self.message = f"File '{file_path}' was not found."
        super().__init__(self.message) 