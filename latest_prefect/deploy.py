import subprocess

# Path to batch script
script_path = "latest_prefect\\config_deploy.bat"

DEPLOYMENT_NAME = 'HaykIsTesting'
FILE_NAME = 'csv_util'
FLOW_NAME = 'csv_manipulation_flow'

try:
    # Run the batch script
    subprocess.run([script_path,DEPLOYMENT_NAME,FILE_NAME,FLOW_NAME], 
                   shell=True, check=True)

except subprocess.CalledProcessError as e:
    print(f"Error: {e}")