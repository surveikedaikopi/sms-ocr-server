#!/bin/bash
python scto_updater.py 2>&1 &
exec uvicorn main:app --host 0.0.0.0 --port 8008