import json
import time
import numpy as np
from fastapi import Form, Request, HTTPException
from typing import List

from config.config import *

# Dictionary to store request timestamps for rate limiting
request_timestamps = {}
TIME_WINDOW = 60  # Time window in seconds for rate limiting




async def receive_media_info(
    media: List[str] = Form(...),
    ip_address: List[List[str]] = Form(...),
    event_id: List[List[str]] = Form(...)
):
    """
    Receives a list of media, a list of lists of IP addresses, and a list of lists of event IDs,
    merges all IP addresses, and saves the IPs and IP-address-event ID mapping to JSON files.
    """
    try:
        # Ensure the inputs are valid
        if not isinstance(media, list) or not all(isinstance(ip_list, list) for ip_list in ip_address) or not all(isinstance(event_list, list) for event_list in event_id):
            raise HTTPException(status_code=400, detail="Invalid format in one of the inputs.")
        
        if len(media) != len(ip_address) or len(media) != len(event_id):
            raise HTTPException(status_code=400, detail="The number of media entries must match the number of IP address lists and event ID lists.")
        
        # Flatten the list of lists of IP addresses and remove duplicates
        all_ips = list(set(ip for sublist in ip_address for ip in sublist))
        
        # Create new IP-address-event ID mapping
        ip_event_mapping = {}
        for ip_list, event_list in zip(ip_address, event_id):
            for ip in ip_list:
                if ip not in ip_event_mapping:
                    ip_event_mapping[ip] = set()
                ip_event_mapping[ip].update(event_list)
        
        # Convert sets to lists for JSON serialization
        ip_event_mapping = {ip: list(events) for ip, events in ip_event_mapping.items()}
        
        # Write the new list of IPs to the file
        with open(f"{local_disk}/ip_whitelist.json", "w") as file:
            json.dump(all_ips, file)
        
        # Write the new IP-address-event ID mapping to the file
        with open(f"{local_disk}/ip_address_eventid.json", "w") as file:
            json.dump(ip_event_mapping, file)
        
        return {"message": "IP whitelist and IP-address-event ID mapping updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





async def quickcount_kedaikopi(request: Request):
    """
    Handles quick count requests.
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






# async def region_aggregate(
#     part_sum: list = Form(...), 
#     total_sum: list = Form(...),
# ):
#     """
#     Aggregates regional data by calculating the percentage of part_sum over total_sum.
#     """
#     part_sum = [int(value) for element in part_sum for value in element.split(",")]
#     total_sum = [int(value) for element in total_sum for value in element.split(",")]
#     result = list(np.round(np.array(part_sum) / np.array(total_sum) * 100, 2))
#     return {"result": result}