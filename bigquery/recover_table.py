import subprocess
import time

# Get current time in seconds since epoch
current_time_seconds = int(time.time())

# Calculate the snapshot time
snapshot_time = int((current_time_seconds - 120) * 1000)

# Construct the bq command
bq_command = [
    "bq",
    "--location=EU",
    "cp",
    "ch10eu.restored_cycle_stations@{}".format(snapshot_time),
    "ch10eu.restored_table"
]

# Execute the bq command
subprocess.run(bq_command)