#!/bin/sh

cd /opt/spark/work-dir/

# Run the Python script
echo "Initializing delta table..."
python3 ./godata2023/delta_tables/create_bronze_layer.py && python3 ./godata2023/delta_tables/create_silver_layer.py

# Start cron in the background
cron

# Tail the log file to keep the container running
tail -f /var/log/cron.log
