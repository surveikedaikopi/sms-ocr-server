#!/bin/bash
python scto_updater.py >> output.log &
exec uvicorn main:app --host 0.0.0.0 --port 8008