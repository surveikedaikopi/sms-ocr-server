#!/bin/bash
python app/scto_updater.py >> output.log &
uvicorn main:app --host 0.0.0.0 --port 8008