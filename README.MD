# Prefect Workflow Automation

This repository contains Prefect flows for automating data processing and scheduling tasks. Prefect orchestrates workflows and allows for efficient task management and monitoring.

## Project Structure

- **/logs**: 

    - The `logger.py` customizes Python's `logging` library to save all logs in the `/logs` folder. It provides centralized logging with easy integration into your Python scripts.

- **/data**:

    - the `data` folder contains all the csv files

## Prefect Configuration

### Start a Prefect Server 

**NOTE:** Create a virtual environment if you don't have one & activate it.
   - *Creating a virtual environment:* `python -m venv venv`
   - *Activating the virtual environment:* `.\venv\Scripts\Activate`

#### Steps to Deploy a Prefect Flow

- **STEP 1:** Open a `powershell` terminal
- **STEP 2:** Run `prefect server start` terminal
- **STEP 3:** Open another `powershell` terminal
- **STEP 4:** Activate your virtual environment
- **STEP 5:** Run `prefect deployment build -n "Thursday CSV Flow" csv_param_test.py:param_flow` 
  - -`n "Thursday CSV Flow"` : Names the deployment
  - `csv_param_test.py:param_flow` : Specifies the module and flow function to build
  - After execution a *.yaml* file is being created (*`param_flow-deployment.yaml`*)
- **STEP 6:** `prefect deployment apply param_flow-deployment.yaml`
  - `param_flow-deployment.yaml` is the file created in the ***STEP 5***  
- **STEP 7:** Run `prefect agent start -q 'default'` in order to start **Prefect Agent**
- **STEP 8:** Open `http://127.0.0.1:4200/` in your browser.


### Contacts

[Github Account](https://github.com/Aleqyan666)  |  [LinkedIn Account](https://www.linkedin.com/in/hayk-alekyan-900797204/)
