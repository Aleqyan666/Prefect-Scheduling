# Prefect Workflow Automation

### *This repository contains Prefect flows for automating data processing and scheduling tasks. Prefect orchestrates workflows and allows for efficient task management and monitoring.*

## Table of Contents

- [Prefect Workflow Automation](#prefect-workflow-automation)
      - [*This repository contains Prefect flows for automating data processing and scheduling tasks. Prefect orchestrates workflows and allows for efficient task management and monitoring.*](#this-repository-contains-prefect-flows-for-automating-data-processing-and-scheduling-tasks-prefect-orchestrates-workflows-and-allows-for-efficient-task-management-and-monitoring)
  - [Table of Contents](#table-of-contents)
  - [Project Structure](#project-structure)
  - [More automated Prefect !!! Latest Prefect Version](#more-automated-prefect--latest-prefect-version)
    - [If you want to start Prefect in your computer in seperate powershell  (different from the below one):](#if-you-want-to-start-prefect-in-your-computer-in-seperate-powershell--different-from-the-below-one)
    - [Optional](#optional)
  - [Prefect (Manual Version) Configuration](#prefect-manual-version-configuration)
    - [Start a Prefect Server](#start-a-prefect-server)
      - [Steps to Deploy a Prefect Flow](#steps-to-deploy-a-prefect-flow)
    - [Changing parameters in prefect](#changing-parameters-in-prefect)
  - [Deploy with ease](#deploy-with-ease)
  - [Cron Scheduling Examples](#cron-scheduling-examples)
    - [**In a Cron Schedule:**](#in-a-cron-schedule)
      - [`<minute> <hour> <day of month> <month> <day of week>`](#minute-hour-day-of-month-month-day-of-week)
  - [Contacts](#contacts)

## Project Structure

---

- **/exceptions:** 

    - the `exceptions/` folder contains custom exception classes for handling specific error cases in this project, improving error management and clarity in the code

- **/data:**

    - the `data/` folder contains all the csv files

- **/logs:** 

    - the `logger.py` customizes Python's `logging` library to save all logs in the `logs/` folder. It provides centralized logging with easy integration into your Python scripts

- **/latest_prefect:**

    Fully automated and improved version of ***/prefect_activation***
    - **`.bat`** files are used to execute commands for starting Prefect and running deployments.
    - **`.py`** files are designed to execute these **`.bat`** files.

- **/prefect_activation:**

    More manual version of Prefect
    - **`.bat`** files are used to execute commands for starting Prefect and running deployments.
    - **`.py`** files are designed to execute these **`.bat`** files. 

---

## More automated Prefect !!! Latest Prefect Version

  ### If you want to start Prefect in your computer in seperate powershell  (different from the below one):

no hard coding should be done do it in *`pool_activate.bat`*
- ***NOTE:** Make sure that in `.\latest_prefect\pool_activate.bat` is set right value of **`pool`***

- **STEP 1:** Open ***powershell terminal*** and Run `python .\latest_prefect\run.py`

- **STEP 2:** Open `.\latest_prefect\deploy.py` and fill out the values
  - **DEPLOYMENT_NAME**: Name of the deployment: `Data_Ingestion_Pipeline`
  - **FILE_NAME**: Name of the file: `csv_util` it is a .py file
  - **FLOW_NAME**: Name of the flow: `csv_manipulation_flow` The name of function with *`@flow`* annotation that should be executed 
  - **CRON_SCHEDULE** = The schedule of deployment: `'2 2 * * 2'`  
      - *More on scheduling here:* [Cron Scheduling Examples](#cron-scheduling-examples)
  - **TIMEZONE**: Name of the timezone:`'Asia/Yerevan'`
  - **WORK_POOL**: Name of the pool: `'first_pool'`

- **STEP 3:** Open ***powershell terminal*** and Run `python .\latest_prefect\deploy.py`

- **STEP 4:** Open `http://127.0.0.1:4200/` in your browser. 

### Optional
- **STEP 4:** Go to the **`Deployments`** section

- **STEP 5:** Click on the name of the *deployment*

- **STEP 6:** Click on **`⋮`** in the top right corner and select **`Edit`**

- **STEP 6:** Click **`Run`** in the top right corner

- **STEP 7:** Choose **`Quick Run`** for immediate execution or **`Custom Run`** for more options
 
- **STEP 8**: Click **`Run`** to start the process

---

## Prefect (Manual Version) Configuration 

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
---
Sometimes, you may need to adjust variable values. For example, if a flow executes a function that sends a message to User A when an email is received, you might also want to send it to User B. In this case, you can simply change the value of the parameter in the Prefect UI.

After completing the steps(**8 Steps**) above, you may need to adjust the flow's parameter values (assuming the flow accepts arguments). To do this, navigate to the Prefect UI and assign the desired value to the parameter. Here's how to proceed:

---

- **STEP 1:** Go to the **`Deployments`** section

- **STEP 2:** Click on **`⋮`** in the top right corner and select **`Edit`**

- **STEP 3:** Scroll down to the **`Parameters`** section and update the parameter values

- **STEP 4:** Enable **`Enforce Parameter Schema`**

- **STEP 5:** Click **`Save`** at the bottom right

- **STEP 6:** Click **`Run`** in the top right corner

- **STEP 7:** Choose **`Quick Run`** for immediate execution or **`Custom Run`** for more options
 
- **STEP 8**: Click **`Run`** to start the process

## Deploy with ease

---

- **STEP 1:** Run in powershell `python .\prefect_activation\prefect_run.py` 

- **STEP 2:** Run in another powershell `python .\prefect_activation\prefect_deployment.py` 
  
- **STEP 3:** After ***STEP 2*** provide values of `deployment_name`, `file_name`, `flow_name` seperately
  
- **STEP 4:** Open `http://127.0.0.1:4200/` in your browser.

## Cron Scheduling Examples

---

**Cron** is a useful tool for automating tasks by scheduling commands or scripts to run at specific times or intervals. Below are examples of how to set up cron schedules for particular scenarios.

### **In a Cron Schedule:**
#### `<minute> <hour> <day of month> <month> <day of week>`

***Example 1:*** 
  - Schedule a Task to Run Every Day at 8:05 PM : `5 20 * * *`

***Example 2:***
  - Schedule a Task to Run Every Wednesday: `* * * * 3`

***Example 3:***
  - Schedule a Task to Run Every Friday at 6:00 PM: `0 18 * * 5`

***Example 4:***
  - Schedule a Task to Run Every 15 Minutes: `*/15 * * * *`

***Example 5:***
  - Schedule a Task to Run at 7:00 AM on the 1st and 15th of Every Month: `0 7 1,15 * *`

***Example 6:***
  - Schedule a Task to Run at 5:30 PM Every Day in July: `30 17 * 7 *`

***Example 7:***
  - Schedule a Task to Run at 12:00 PM on the 10th of June: `0 12 10 6 *`
  
***Example 8:***
  - Schedule a Task to Run at 9:00 AM Every Day in March, June, and September: `0 9 * 3,6,9 *`

## Contacts

[Github Account](https://github.com/Aleqyan666)     |      [LinkedIn Account](https://www.linkedin.com/in/hayk-alekyan-900797204/)
