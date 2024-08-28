@echo off
REM Activate the virtual environment.
call .venv\Scripts\Activate

REM Move back to the project root directory.
cd ../..

REM Start the Prefect server.
prefect server start