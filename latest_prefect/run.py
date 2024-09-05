import subprocess

# Paths to batch files
bat_file_1 = "latest_prefect\\server_start.bat"
bat_file_2 = "latest_prefect\\pool_activate.bat"
# Start both .bat files in separate terminals
process_1 = subprocess.Popen(["start", "cmd", "/K", bat_file_1], shell=True)
process_2 = subprocess.Popen(["start", "cmd", "/K", bat_file_2], shell=True)
# Optionally, wait for both processes to finish
process_1.wait()
process_2.wait()

