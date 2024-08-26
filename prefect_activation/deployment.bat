@echo off
REM Activate the virtual environment.
call .venv\Scripts\Activate

REM Prompting the user for input
set /p deployment_name="Enter the Deployment name (e.g., Thursday Flow): "
set /p file_name="Enter the File name (e.g., csv_param_test): "
set /p flow_name="Enter the Flow name (e.g., param_flow): "

REM Running the Prefect deployment build command
echo Waiting 5 seonds before building the Prefect deployment...
timeout /t 5 /nobreak
prefect deployment build -n "%deployment_name%" "%file_name%.py:%flow_name%"

REM Applying the deployment file after a 30-second delay
echo Waiting 10 seconds before applying the deployment...
timeout /t 10 /nobreak
prefect deployment apply "%flow_name%-deployment.yaml"

REM Starting the Prefect Agent
prefect agent start -q "default"
@echo
@echo
@echo
pause