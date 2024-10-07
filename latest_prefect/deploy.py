import subprocess

# Path to batch script
script_path = "latest_prefect\\config_deploy.bat"

DEPLOYMENT_NAME = 'Parameter_testing22'
FILE_NAME = 'variables2'
FLOW_NAME = 'run_create_variables'
CRON_SCHEDULE = '2 2 * * 2'
TIMEZONE = 'Asia/Yerevan'
WORK_POOL = 'first_pool'

try:
    # Run the batch script
    subprocess.run([script_path, DEPLOYMENT_NAME, FILE_NAME, FLOW_NAME, CRON_SCHEDULE, TIMEZONE, WORK_POOL], 
                   shell=True, check=True)

except subprocess.CalledProcessError as e:
    print(f"Error: {e}")