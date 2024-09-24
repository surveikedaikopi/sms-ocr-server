import json
import time
import pandas as pd
from typing import List
from fastapi import Request, HTTPException

from pydantic import BaseModel


from config.config import *

# Dictionary to store request timestamps for rate limiting
request_timestamps = {}





# Define the structure of the incoming data
class MediaInfo(BaseModel):
    media: List[str]
    ip_address: List[str]
    event_id: List[str]

async def receive_media_info(media_info: List[MediaInfo]):
    """
    Receives a list of media, a list of IP addresses, and a list of event IDs,
    converts IP addresses and event IDs to lists of lists, merges all IP addresses,
    and saves the IPs and IP-address-event ID mapping to JSON files.
    """
    try:
        # Convert ip_address and event_id to lists of lists of strings
        for item in media_info:
            item.ip_address = [ip.split(',') for ip in item.ip_address]
            item.event_id = [event.split(',') for event in item.event_id]

        # Flatten the list of all IP addresses and remove duplicates
        all_ips = list(set(ip for item in media_info for sublist in item.ip_address for ip in sublist))

        # Create new IP-address-event ID mapping
        ip_event_mapping = {}
        for item in media_info:
            for ip_list, event_list in zip(item.ip_address, item.event_id):
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
    if current_time - last_request_time < RATE_LIMIT_TIME_WINDOW:
        raise HTTPException(status_code=429, detail="Too Many Requests")

    # Update the request timestamp for the client
    request_timestamps[client_ip] = current_time

    try:
        # Get event IDs attributed to the client_ip from ip_address_eventid.json
        with open(f"{local_disk}/ip_address_eventid.json", "r") as file:
            ip_event_mapping = json.load(file)
        
        if client_ip not in ip_event_mapping:
            raise HTTPException(status_code=403, detail="No event IDs attributed to this IP")

        event_ids = ip_event_mapping[client_ip]

        # Read and filter results_quickcount.csv based on selected event IDs using pandas
        df = pd.read_csv(f"{local_disk}/results_quickcount.csv")
        filtered_df = df[df['event_id'].isin(event_ids)]

        filtered_results = []
        for _, row in filtered_df.iterrows():
            event_id = row['event_id']
            event_file = f"{local_disk}/event_{event_id}.json"
            if not os.path.exists(event_file):
                continue

            with open(event_file, "r") as file:
                event_info = json.load(file)
                n_candidate = event_info.get("n_candidate", 0)

            result = {
                "event_id": row['event_id'],
                "event_name": row['event_name'],
                "event_type": row['event_type'],
                "region": row['region']
            }
            for i in range(1, n_candidate + 1):
                result[f"0{i}"] = row[f"vote{i}_pct"]

            filtered_results.append(result)

        return {
            "timestamp": current_time,
            "results": filtered_results
        }
    except FileNotFoundError:
        return {"message": "File not found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




