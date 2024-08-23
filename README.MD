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

### Changing parameters in prefect
Sometimes, you may need to adjust variable values. For example, if a flow executes a function that sends a message to User A when an email is received, you might also want to send it to User B. In this case, you can simply change the value of the parameter in the Prefect UI.

After completing the steps(**8 Steps**) above, you may need to adjust the flow's parameter values (assuming the flow accepts arguments). To do this, navigate to the Prefect UI and assign the desired value to the parameter. Here's how to proceed:

- **STEP 1:** Go to the **`Deployments`** section

- **STEP 2:** Click on **`⋮`** in the top right corner and select **`Edit`**

- **STEP 3:** Scroll down to the **`Parameters`** section and update the parameter values

- **STEP 4:** Enable **`Enforce Parameter Schema`**

- **STEP 5:** Click **`Save`** at the bottom right

- **STEP 6:** Click **`Run`** in the top right corner

- **STEP 7:** Choose **`Quick Run`** for immediate execution or **`Custom Run`** for more options
 
- **STEP 8**: Click **`Run`** to start the process

### Contacts

[Github Account](https://github.com/Aleqyan666)     |      [LinkedIn Account](https://www.linkedin.com/in/hayk-alekyan-900797204/)
