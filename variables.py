import os
from prefect import get_run_logger, task, flow
from prefect.variables import Variable
import logging
from logger import bcolors, CustomFormatter
import datetime
from datetime import timedelta
import exceptions.VariableNotFoundException as VariableNotFoundException

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


"""
Prefect Variable Management

This script provides functionality for managing Prefect variables, including:
- Adding new variables to Prefect.
- Retrieving the values of existing variables.
"""


@task(persist_result=True, log_prints=True) # Adding a variable into Prefect Ui
def set_prefect_variable(key: str, value: str):
    """
    Creates or updates a Prefect variable if it doesn't already exist.
    
    Args:
        key (str): The name of the variable.
        value (str): The value to set for the variable.
    """
    logger = get_run_logger()
    try:
        # Try to get the existing variable
        existing_value = Variable.get(key, default=None)
        if existing_value is not None:
            logger.info(f"Variable '{key}' already exists with value: {existing_value}. Skipping creation.")
        else:
            # If variable doesn't exist, create it
            Variable.set(key, value)
            logger.info(f"Variable '{key}' has been set to: {value}")
    except Exception as e:
        logger.error(f"Error retrieving or setting variable: {e}")

    
@task(persist_result=True, log_prints=True)
def set_prefect_emails_list(users: list):
    """
    Retrieves the value of a Prefect variable by its name.

    This task attempts to retrieve a variable from Prefect using the provided variable name. If the
    variable is found, its value is logged and returned. If the variable does not exist, an error message
    is logged, and `None` is returned.

    Args:
        var_name (str): The name of the variable to retrieve from Prefect. This should be the exact key
                        used to store the variable in Prefect.

    Returns:
        str: The value of the retrieved variable if found; `None` if the variable does not exist.

    Raises:
        VariableNotFoundException: If the specified variable does not exist in Prefect, an error is
                                   logged, and `None` is returned.
    """
    logger = get_run_logger()

    for user in users:
        value = str(user)
        key = f"mail_{user.split('@viva.am')[0]}"

        try:
            # Check if the variable already exists
            existing_value = Variable.get(key, default=None)
            if existing_value is not None:
                logger.info(f"Variable '{key}' already exists with value: {existing_value}. Skipping creation.")
            else:
                # If variable doesn't exist, create it
                Variable.set(key, value)
                logger.info(f"Variable '{key}' has been set to: {value}")
        except Exception as e:
            logger.error(f"Error retrieving or setting variable '{key}': {e}")


@task(persist_result=True, log_prints=True)
def get_email_by_username(username: str) -> str:
    """
    Retrieves the email address associated with a given username from Prefect.

    Args:
        username (str): The username to use as the key for the Prefect variable.
                        The variable name is prefixed with 'mail_'.

    Returns:
        str: The email address if the variable is found; `None` if not found.
    """
    logger = get_run_logger()
    key = f"mail_{username}"

    logger.info(f"Retrieving the variable '{key}' from Prefect/Variables")
    try:
        my_var = Variable.get(key)
        logger.info(f"Variable name: '{key}', Value: '{my_var}'")
        return my_var
    except VariableNotFoundException as e:
        logger.error(f"Error retrieving variable '{key}': {e}")
        return None


@flow(name="Create Variables",
       flow_run_name="Artak",
         log_prints=True)  
def create_flow():

    # get_email_by_username('halekyan')
    user_list = ['lauramartinez@viva.am', 'jessicalee@viva.am', 'sarahtaylor@viva.am', 
                 'johnsmith@viva.am', 'emilydavis@viva.am']
    # set_prefect_emails_list(user_list) #Adding all these user/mail values into Prefect with 
    # set_prefect_variable("mail_talekyan", "talekyan@viva.am")
    # set_prefect_variable("query_3", "SELECT * FROM sub_device")





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

    create_flow()

    for handler in logger.handlers:
        handler.flush()
        handler.close()