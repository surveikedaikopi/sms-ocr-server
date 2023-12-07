#!/bin/bash
python scto_updater.py > /app/logs/scto_updater.log 2>&1 &
exec uvicorn main:app --host 0.0.0.0 --port 8008 2>&1 | tee -a /app/logs/uvicorn.log