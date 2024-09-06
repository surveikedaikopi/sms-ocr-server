import os
import json
import time
import requests
from fastapi import Form, Request, HTTPException
import numpy as np

# Configuration import
from config.config import local_disk, headers

# Dictionary to store request timestamps for rate limiting
request_timestamps = {}
TIME_WINDOW = 60  # Time window in seconds for rate limiting

async def receive_ip_whitelist(request: Request):
    """
    Receives a list of IP addresses to whitelist and saves it to a JSON file.
    """
    try:
        # Parse the JSON body from the request
        body = await request.json()
        
        # Ensure the body contains a list of IPs
        if not isinstance(body, list):
            raise HTTPException(status_code=400, detail="Invalid format. Expected a list of IP addresses.")
        
        # Write the list of IPs to the file
        with open(f"{local_disk}/ip_whitelist.json", "w") as file:
            json.dump(body, file)
        
        return {"message": "IP whitelist updated successfully"}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def pilpres_quickcount_kedaikopi(request: Request):
    """
    Handles quick count requests for Pilpres (Presidential Election).
    Checks if the client IP is whitelisted and rate limits requests.
    """
    client_ip = request.headers.get("X-Forwarded-For", "").split(', ')[0]
    print(f'Client IP: {client_ip}')

    try:
        with open(f"{local_disk}/ip_whitelist.json", "r") as file:
            whitelist = json.load(file)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="IP whitelist file not found")

    if client_ip not in whitelist:
        # Return Forbidden status code if client is not whitelisted
        raise HTTPException(status_code=403, detail="Access Forbidden")

    # Get the current timestamp
    current_time = time.time()

    # Check if the client has made a request within the last minute
    last_request_time = request_timestamps.get(client_ip, 0)
    if current_time - last_request_time < TIME_WINDOW:
        raise HTTPException(status_code=429, detail="Too Many Requests")

    # Update the request timestamp for the client
    request_timestamps[client_ip] = current_time

    try:
        with open(f'{local_disk}/results_pilpres_quickcount.json', 'r') as json_file:
            data_read = json.load(json_file)
        return {"results": data_read}
    except FileNotFoundError:
        return {"message": "File not found"}

async def pilkada_quickcount_kedaikopi(request: Request):
    """
    Handles quick count requests for Pilkada (Regional Election).
    Checks if the client IP is whitelisted and rate limits requests.
    """
    client_ip = request.headers.get("X-Forwarded-For", "").split(', ')[0]
    print(f'Client IP: {client_ip}')

    try:
        with open(f"{local_disk}/ip_whitelist.json", "r") as file:
            whitelist = json.load(file)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="IP whitelist file not found")

    # Check if the client is whitelisted
    if client_ip not in whitelist:
        # Return Forbidden status code if client is not whitelisted
        raise HTTPException(status_code=403, detail="Access Forbidden")

    current_time = time.time()

    # Check if the client has made a request within the last minute
    last_request_time = request_timestamps.get(client_ip, 0)
    if current_time - last_request_time < TIME_WINDOW:
        raise HTTPException(status_code=429, detail="Too Many Requests")

    # Update the request timestamp for the client
    request_timestamps[client_ip] = current_time

    try:
        with open(f'{local_disk}/results_pilkada_quickcount.json', 'r') as json_file:
            data_read = json.load(json_file)
        return {"results": data_read}
    except FileNotFoundError:
        return {"message": "File not found"}

async def read_sms_inbox():
    """
    Reads the SMS inbox from a JSON file and returns the data.
    """
    try:
        with open(f"{local_disk}/sms_inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"sms_inbox": data}
    except FileNotFoundError:
        return {"message": "File not found"}

async def read_wa_inbox():
    """
    Reads the WhatsApp inbox from a JSON file and returns the data.
    """
    try:
        with open(f"{local_disk}/wa_inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"wa_inbox": data}
    except FileNotFoundError:
        return {"message": "File not found"}

async def region_aggregate(
    part_sum: list = Form(...), 
    total_sum: list = Form(...),
):
    """
    Aggregates regional data by calculating the percentage of part_sum over total_sum.
    """
    part_sum = [int(value) for element in part_sum for value in element.split(",")]
    total_sum = [int(value) for element in total_sum for value in element.split(",")]
    result = list(np.round(np.array(part_sum) / np.array(total_sum) * 100, 2))
    return {"result": result}