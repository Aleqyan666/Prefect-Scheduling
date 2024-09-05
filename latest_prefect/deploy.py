import subprocess

# Path to batch script
script_path = "latest_prefect\\config_deploy.bat"

DEPLOYMENT_NAME = 'Noway'
FILE_NAME = 'csv_util'
FLOW_NAME = 'csv_manipulation_flow'
CRON_SCHEDULE = '2 2 * * 2'
TIMEZONE = 'Asia/Yerevan'
WORK_POOL = 'first_pool'

try:
    # Run the batch script
    subprocess.run([script_path, DEPLOYMENT_NAME, FILE_NAME, FLOW_NAME, CRON_SCHEDULE, TIMEZONE, WORK_POOL], 
                   shell=True, check=True)

except subprocess.CalledProcessError as e:
    print(f"Error: {e}")