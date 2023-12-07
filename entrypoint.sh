#!/bin/bash

# Create log directories
mkdir -p /app/logs

# Start SCTO updater in background and redirect output to a log file
python scto_updater.py > /app/logs/scto_updater.log 2>&1 &

# Start uvicorn and append output to another log file
exec uvicorn main:app --host 0.0.0.0 --port 8008 2>&1 | tee -a /app/logs/uvicorn.log