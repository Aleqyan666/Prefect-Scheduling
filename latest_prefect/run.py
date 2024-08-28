import subprocess

# Path to batch script
script_path = "latest_prefect\\server_start.bat"

try:
    # Run the batch script
    subprocess.run(script_path, shell=True, check=True)

except subprocess.CalledProcessError as e:
    print(f"Error: {e}")
