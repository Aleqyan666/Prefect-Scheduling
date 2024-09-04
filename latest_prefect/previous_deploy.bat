@echo off
REM Activate the virtual environment.
call .venv\Scripts\Activate
REM Prompting the user for input
set  DEPLOYMENT_NAME=%1
set  FILE_NAME=%2
set  FLOW_NAME=%3

prefect deploy -n "%DEPLOYMENT_NAME%" "%FILE_NAME%.py:%FLOW_NAME%"

REM Starting the Prefect Agent
prefect worker start -q "default"
@echo
@echo
@echo
pause