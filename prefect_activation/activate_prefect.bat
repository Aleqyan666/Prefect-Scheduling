@echo off
cd .venv\Scripts 
call Activate
cd ../..
prefect server start