@echo off
REM Navigate to the 'Scripts' directory inside your virtual environment.
REM Make sure the path to '.venv\Scripts' matches your virtual environment's location.
cd .venv\Scripts 

REM Activate the virtual environment.
call Activate

REM Move back to the project root directory.
cd ../..

REM Start the Prefect server.
prefect server start