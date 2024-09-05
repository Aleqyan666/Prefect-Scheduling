REM Activate the virtual environment.
call .venv\Scripts\Activate
echo venv is activated...
prefect worker start --pool first_pool
@REM No hardcoding should be used for giving the parameter