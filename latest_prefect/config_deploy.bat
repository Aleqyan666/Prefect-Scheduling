@echo off
REM Activate the virtual environment.
call .venv\Scripts\Activate

REM Prompting the user for input or use default values
set DEPLOYMENT_NAME=%1
set FILE_NAME=%2
set FLOW_NAME=%3
set CRON_SCHEDULE=%4
set TIMEZONE=%5
set WORK_POOL=%6

REM Deploy the flow with the provided parameters and automatically answer prompts
(
    echo n
    echo n
) | prefect deploy -n "%DEPLOYMENT_NAME%" "%FILE_NAME%.py:%FLOW_NAME%" --cron %CRON_SCHEDULE% --timezone %TIMEZONE% --pool %WORK_POOL%

REM Starting the Prefect Worker with the specified work pool
prefect worker start --pool "%WORK_POOL%" 

@echo
@echo
@echo
pause