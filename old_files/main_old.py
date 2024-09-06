import warnings
warnings.filterwarnings("ignore", module="google.oauth2")

import os
import json
import time
import tools
import requests
import threading
import numpy as np
import pandas as pd
import concurrent.futures
from typing import Optional
from dotenv import load_dotenv
from collections import defaultdict
from pysurveycto import SurveyCTOObject
from datetime import datetime, timedelta
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Form, FastAPI, UploadFile, Request, HTTPException


# ================================================================================================================
# Initial Setup

# Load env
load_dotenv('.env')

# Define app
app = FastAPI(docs_url="/docs", redoc_url=None)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dictionary to store request timestamps for each client IP
request_timestamps = defaultdict(float)

# Time window in seconds
TIME_WINDOW = 60  # 1 minute

# Global Variables
url_send_sms = os.environ.get('url_send_sms')
url_send_wa = os.environ.get('url_send_wa')
url_bubble = os.environ.get('url_bubble')
url_getUID = os.environ.get('url_getUID')
local_disk = os.environ.get('local_disk')
BUBBLE_API_KEY = os.environ.get('BUBBLE_API_KEY')
SCTO_SERVER_NAME = os.environ.get('SCTO_SERVER_NAME')
SCTO_USER_NAME = os.environ.get('SCTO_USER_NAME')
SCTO_PASSWORD = os.environ.get('SCTO_PASSWORD')
NUSA_USER_NAME = os.environ.get('NUSA_USER_NAME')
NUSA_PASSWORD = os.environ.get('NUSA_PASSWORD')
NUSA_API_KEY = os.environ.get('NUSA_API_KEY')
list_WhatsApp_Gateway = {
    1: os.environ.get('WA_GATEWAY_1'),
    2: os.environ.get('WA_GATEWAY_2'),
    3: os.environ.get('WA_GATEWAY_3'),
    4: os.environ.get('WA_GATEWAY_4'),
    5: os.environ.get('WA_GATEWAY_5'),
    6: os.environ.get('WA_GATEWAY_6'),
    7: os.environ.get('WA_GATEWAY_7'),
    8: os.environ.get('WA_GATEWAY_8'),
    9: os.environ.get('WA_GATEWAY_9'),
    10: os.environ.get('WA_GATEWAY_10'),
    11: os.environ.get('WA_GATEWAY_11'),
    12: os.environ.get('WA_GATEWAY_12'),
    13: os.environ.get('WA_GATEWAY_13'),
    14: os.environ.get('WA_GATEWAY_14'),
    15: os.environ.get('WA_GATEWAY_15'),
    16: os.environ.get('WA_GATEWAY_16')
}

# Bubble Headers
headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}


# ================================================================================================================
# Endpoint to receive ip_whitelist
@app.post("/receive_ip_whitelist")
async def receive_ip_whitelist(request: Request):
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




# ================================================================================================================
# Endpoint to read the quick count results (PILPRES)
@app.get("/api/pilpres_quickcount_kedaikopi")
async def pilpres_quickcount_kedaikopi(request: Request):
    client_ip = request.headers.get("X-Forwarded-For").split(', ')[0]
    print(f'Client IP: {client_ip}')

    # IP Whitelist
    with open(f"{local_disk}/ip_whitelist.json", "r") as file:
        whitelist = json.load(file)

    # Check if the client is whitelisted
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



# ================================================================================================================
# Endpoint to read the quick count results (PILKADA)
@app.get("/api/pilkada_quickcount_kedaikopi")
async def pilkada_quickcount_kedaikopi(request: Request):
    client_ip = request.headers.get("X-Forwarded-For").split(', ')[0]
    print(f'Client IP: {client_ip}')

    # IP Whitelist
    with open(f"{local_disk}/ip_whitelist.json", "r") as file:
        whitelist = json.load(file)

    # Check if the client is whitelisted
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
        with open(f'{local_disk}/results_pilkada_quickcount.json', 'r') as json_file:
            data_read = json.load(json_file)
        return {"results": data_read}
    except FileNotFoundError:
        return {"message": "File not found"}


# ================================================================================================================
# Endpoint to read the "sms_inbox" file
@app.get("/sms_inbox")
async def read_sms_inbox():
    try:
        with open(f"{local_disk}/sms_inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"inbox_data": data}
    except FileNotFoundError:
        return {"message": "File not found"}


# ================================================================================================================
# Endpoint to read the "sms_inbox" file
@app.get("/wa_inbox")
async def read_wa_inbox():
    try:
        with open(f"{local_disk}/wa_inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"inbox_data": data}
    except FileNotFoundError:
        return {"message": "File not found"}



# ================================================================================================================
# Endpoint to receive SMS message, to validate, and to forward the pre-processed data

# Define the number of endpoints
num_sms_endpoints = 16

# Endpoint to receive SMS message, to validate, and to forward the pre-processed data
for port in range(1, num_sms_endpoints + 1):
    @app.post(f"/sms-receive-{port}")
    async def receive_sms(
        request: Request,
        id: str = Form(...),
        gateway_number: str = Form(...),
        originator: str = Form(...),
        msg: str = Form(...),
        receive_date: str = Form(...)
    ):

        # Extract the port number from the request
        port = request.url.path.split('-')[-1]
        
        # Create a dictionary to store the data
        raw_data = {
            "ID": id,
            "Gateway Port": port,
            "Gateway ID": gateway_number,
            "Sender": originator,
            "Message": msg,
            "Receive Date": receive_date
        }

        # Log the received data to a JSON file
        with open(f"{local_disk}/sms_inbox.json", "a") as json_file:
            json.dump(raw_data, json_file)
            json_file.write('\n')  # Add a newline to separate the JSON objects

        # Split message and remove spaces
        info = [part.strip() for part in msg.lower().split('#')]

        # Default Values
        error_type = None
        raw_sms_status = 'Rejected'

        # Check Error Type 1 (prefix)
        if info[0] == 'kk':

            try:
                uid = info[1].lower()
                event = info[2].lower()

                # Get number of candidate pairs
                with open(f'{local_disk}/event_{event}.json', 'r') as json_file:
                    json_content = json.load(json_file)
                    number_candidates = json_content['n_candidate']

                format = 'KK#UID#EventID#' + '#'.join([f'0{i+1}' for i in range(number_candidates)]) + '#Rusak'
                template_error_msg = 'cek & kirim ulang dgn format:\n' + format

                tmp = pd.read_excel(f'{local_disk}/target_{event}.xlsx', usecols=['UID'])

                # Check Error Type 2 (UID)
                if uid not in tmp['UID'].str.lower().tolist():
                    message = f'UID "{uid.upper()}" tidak terdaftar, ' + template_error_msg
                    error_type = 2
                else:
                    # Check Error Type 3 (data completeness)
                    if len(info) != number_candidates + 4:
                        message = 'Data tidak lengkap, ' + template_error_msg
                        error_type = 3
                    else:
                        # Get votes
                        votes = np.array(info[3:-1]).astype(int)
                        vote1 = votes[0]
                        vote2 = votes[1]
                        try:
                            vote3 = votes[2]
                        except:
                            vote3 = None
                        try:
                            vote4 = votes[3]
                        except:
                            vote4 = None
                        try:
                            vote5 = votes[4]
                        except:
                            vote5 = None
                        try:
                            vote6 = votes[5]
                        except:
                            vote6 = None
                        # Get invalid votes
                        invalid = info[-1]
                        # Get total votes
                        total_votes = np.array(votes).astype(int).sum() + int(invalid)
                        summary = f'EventID: {event}\n' + '\n'.join([f'Paslon0{i+1}: {votes[i]}' for i in range(number_candidates)]) + f'\nTidak Sah: {invalid}' + f'\nTotal: {total_votes}\n'

                        # Check Error Type 4 (maximum votes)
                        if total_votes > 600:
                            message = summary + 'Jumlah suara melebihi 600, ' + template_error_msg
                            error_type = 4
                        else:
                            message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama:\n' + format

                        # Retrieve data with this UID from Bubble database
                        filter_params = [{"key": "UID", "constraint_type": "equals", "value": uid.upper()}]
                        filter_json = json.dumps(filter_params)
                        params = {"constraints": filter_json}
                        res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
                        data = res.json()
                        data = data['response']['results'][0]

                        # Get existing validator
                        if 'Validator' in data:
                            validator = data['Validator']
                        else:
                            validator = None

                        # Check if SCTO data exists
                        scto = data['SCTO']

                        # If SCTO data exists, check if they are consistent
                        if scto:
                            if (np.array_equal(np.array(votes).astype(int), np.array(data['SCTO Votes']).astype(int))) and (int(invalid) == int(data['SCTO Invalid'])):
                                status = 'Verified'
                                validator = 'System'
                            else:
                                status = 'Not Verified'
                        else:
                            status = 'SMS Only'
                        
                        # Extract the hour as an integer
                        tmp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                        hour = tmp.hour
                        
                        # Delta Time
                        if 'SCTO Timestamp' in data:
                            sms_timestamp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                            scto_timestamp = datetime.strptime(data['SCTO Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
                            delta_time = abs(scto_timestamp - sms_timestamp)
                            delta_time_hours = delta_time.total_seconds() / 3600
                        else:
                            delta_time_hours = None

                        # Total Votes
                        total_votes = 0
                        for v in votes:
                            total_votes += int(v) if v is not None else 0

                        # Payload
                        payload = {
                            'Active': True,
                            'SMS': True,
                            'SMS Int': 1,
                            'UID': uid.upper(),
                            'SMS Gateway Port': port,
                            'SMS Gateway ID': gateway_number,
                            'SMS Sender': originator,
                            'SMS Timestamp': receive_date,
                            'SMS Hour': hour,
                            'Event ID': event,
                            'SMS Votes': votes,
                            'SMS Invalid': invalid,
                            'Vote1': vote1,
                            'Vote2': vote2,
                            'Vote3': vote3,
                            'Vote4': vote4,
                            'Vote5': vote5,
                            'Vote6': vote6,
                            'Total Votes': total_votes,
                            'Final Votes': votes,
                            'Invalid Votes': invalid,
                            'Complete': scto,
                            'Status': status,
                            'Delta Time': delta_time_hours,
                            'Validator': validator
                        }

                        raw_sms_status = 'Accepted'

                        # Load the JSON file into a dictionary
                        with open(f'{local_disk}/uid_{event}.json', 'r') as json_file:
                            uid_dict = json.load(json_file)

                        # Forward data to Bubble database
                        _id = uid_dict[uid.upper()]
                        requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)

            except Exception as e:
                error_type = 1
                message = 'Format tidak dikenali. Kirim ulang dengan format yg sudah ditentukan. Contoh utk 3 paslon:\nKK#UID#EventID#01#02#03#Rusak'
                print(f'Error Location: SMS - Error Type 1, keyword: {e}')

            # Return the message to the sender via SMS Masking
            params = {
                "user": NUSA_USER_NAME,
                "password": NUSA_PASSWORD,
                "SMSText": message,
                "GSM": originator,
                "output": "json",
            }
            requests.get(url_send_sms, params=params)

        elif msg == 'the gateway is active':
            # Payload (Gateway Check)
            payload_status = {
                'Gateway Port': port,
                'Gateway Status': True,
                'Last Check': receive_date,
            }

            # Retrieve data with this SIM Number from Bubble database (GatewayCheckSMS)
            filter_params = [{"key": "Gateway ID", "constraint_type": "equals", "value": gateway_number}]
            filter_json = json.dumps(filter_params)
            params = {"constraints": filter_json}
            res = requests.get(f'{url_bubble}/GatewayCheckSMS', headers=headers, params=params)
            data = res.json()
            data = data['response']['results'][0]
            # Forward data to Bubble database (Check Gateway)
            _id = data['_id']
            requests.patch(f'{url_bubble}/GatewayCheckSMS/{_id}', headers=headers, data=payload_status)
            # Set SMS status
            raw_sms_status = 'Check Gateway'
        
        else:
            error_type = 0

        # Payload (RAW SMS)
        payload_raw = {
            'SMS ID': id,
            'Receive Date': receive_date,
            'Sender': originator,
            'Gateway Port': port, 
            'Gateway ID': gateway_number,
            'Message': msg,
            'Error Type': error_type,
            'Status': raw_sms_status
        }

        # Forward data to Bubble database (Raw SMS)
        requests.post(f'{url_bubble}/RAW_SMS', headers=headers, data=payload_raw)




# ================================================================================================================
# Endpoint to receive WhatsApp message, to validate, and to forward the pre-processed data

# Define the number of endpoints
num_whatsapp_endpoints = 16

# Endpoint to receive WhatsApp message, to validate, and to forward the pre-processed data
for port in range(1, num_whatsapp_endpoints + 1):
    @app.post(f"/wa-receive-{port}")
    async def receive_whatsapp(
        request: Request,
        id: str = Form(...),
        gateway_number: str = Form(...),
        originator: str = Form(...),
        msg: str = Form(...),
        receive_date: str = Form(...)
    ):

        # Extract the port number from the request
        port = request.url.path.split('-')[-1]
        
        # Create a dictionary to store the data
        raw_data = {
            "ID": id,
            "Gateway Port": port,
            "Gateway ID": gateway_number,
            "Sender": originator,
            "Message": msg,
            "Receive Date": receive_date
        }

        # Log the received data to a JSON file
        with open(f"{local_disk}/wa_inbox.json", "a") as json_file:
            json.dump(raw_data, json_file)
            json_file.write('\n')  # Add a newline to separate the JSON objects

        # Split message and remove spaces
        info = [part.strip() for part in msg.lower().split('#')]

        # Default Values
        error_type = None
        raw_wa_status = 'Rejected'

        # Check Error Type 1 (prefix)
        if info[0] == 'kk':

            try:
                uid = info[1].lower()
                event = info[2].lower()

                # Get number of candidate pairs
                with open(f'{local_disk}/event_{event}.json', 'r') as json_file:
                    json_content = json.load(json_file)
                    number_candidates = json_content['n_candidate']

                format = 'KK#UID#EventID#' + '#'.join([f'0{i+1}' for i in range(number_candidates)]) + '#Rusak'
                template_error_msg = 'cek & kirim ulang dgn format:\n' + format

                tmp = pd.read_excel(f'{local_disk}/target_{event}.xlsx', usecols=['UID'])

                # Check Error Type 2 (UID)
                if uid not in tmp['UID'].str.lower().tolist():
                    message = f'UID "{uid.upper()}" tidak terdaftar, ' + template_error_msg
                    error_type = 2
                else:
                    # Check Error Type 3 (data completeness)
                    if len(info) != number_candidates + 4:
                        message = 'Data tidak lengkap, ' + template_error_msg
                        error_type = 3
                    else:
                        # Get votes
                        votes = np.array(info[3:-1]).astype(int)
                        vote1 = votes[0]
                        vote2 = votes[1]
                        try:
                            vote3 = votes[2]
                        except:
                            vote3 = None
                        try:
                            vote4 = votes[3]
                        except:
                            vote4 = None
                        try:
                            vote5 = votes[4]
                        except:
                            vote5 = None
                        try:
                            vote6 = votes[5]
                        except:
                            vote6 = None
                        # Get invalid votes
                        invalid = info[-1]
                        # Get total votes
                        total_votes = np.array(votes).astype(int).sum() + int(invalid)
                        summary = f'EventID: {event}\n' + '\n'.join([f'Paslon0{i+1}: {votes[i]}' for i in range(number_candidates)]) + f'\nTidak Sah: {invalid}' + f'\nTotal: {total_votes}\n'

                        # Check Error Type 4 (maximum votes)
                        if total_votes > 600:
                            message = summary + 'Jumlah suara melebihi 600, ' + template_error_msg
                            error_type = 4
                        else:
                            message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama:\n' + format

                        # Retrieve data with this UID from Bubble database
                        filter_params = [{"key": "UID", "constraint_type": "equals", "value": uid.upper()}]
                        filter_json = json.dumps(filter_params)
                        params = {"constraints": filter_json}
                        res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
                        data = res.json()
                        data = data['response']['results'][0]

                        # Get existing validator
                        if 'Validator' in data:
                            validator = data['Validator']
                        else:
                            validator = None

                        # Check if SCTO data exists
                        scto = data['SCTO']

                        # If SCTO data exists, check if they are consistent
                        if scto:
                            if (np.array_equal(np.array(votes).astype(int), np.array(data['SCTO Votes']).astype(int))) and (int(invalid) == int(data['SCTO Invalid'])):
                                status = 'Verified'
                                validator = 'System'
                            else:
                                status = 'Not Verified'
                        else:
                            status = 'SMS Only'
                        
                        # Extract the hour as an integer
                        tmp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                        hour = tmp.hour
                        
                        # Delta Time
                        if 'SCTO Timestamp' in data:
                            sms_timestamp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                            scto_timestamp = datetime.strptime(data['SCTO Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
                            delta_time = abs(scto_timestamp - sms_timestamp)
                            delta_time_hours = delta_time.total_seconds() / 3600
                        else:
                            delta_time_hours = None

                        # Total Votes
                        total_votes = 0
                        for v in votes:
                            total_votes += int(v) if v is not None else 0

                        # Payload
                        payload = {
                            'Active': True,
                            'SMS': True,
                            'SMS Int': 1,
                            'UID': uid.upper(),
                            'SMS Gateway Port': port,
                            'SMS Gateway ID': gateway_number,
                            'SMS Sender': originator,
                            'SMS Timestamp': receive_date,
                            'SMS Hour': hour,
                            'Event ID': event,
                            'SMS Votes': votes,
                            'SMS Invalid': invalid,
                            'Vote1': vote1,
                            'Vote2': vote2,
                            'Vote3': vote3,
                            'Vote4': vote4,
                            'Vote5': vote5,
                            'Vote6': vote6,
                            'Total Votes': total_votes,
                            'Final Votes': votes,
                            'Invalid Votes': invalid,
                            'Complete': scto,
                            'Status': status,
                            'Delta Time': delta_time_hours,
                            'Validator': validator
                        }

                        raw_wa_status = 'Accepted'

                        # Load the JSON file into a dictionary
                        with open(f'{local_disk}/uid_{event}.json', 'r') as json_file:
                            uid_dict = json.load(json_file)

                        # Forward data to Bubble database
                        _id = uid_dict[uid.upper()]
                        requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)

            except Exception as e:
                error_type = 1
                message = 'Format tidak dikenali. Kirim ulang dengan format yg sudah ditentukan. Contoh utk 3 paslon:\nKK#UID#EventID#01#02#03#Rusak'
                print(f'Error Location: WhatsApp - Error Type 1, keyword: {e}')

            # Return the message to the sender via WhatsApp Gatew
            HEADERS_WA = {
                "Accept": "application/json",
                "APIKey": NUSA_API_KEY
            }
            PAYLOADS = {
                'message': message,
                'destination': originator,
                'sender': list_WhatsApp_Gateway[int(port)],
                'include_unsubscribe': False
            }
            requests.post(url_send_wa, headers=HEADERS_WA, json=PAYLOADS)

        elif msg == 'the gateway is active':
            # Payload (Gateway Check)
            payload_status = {
                'Gateway Port': port,
                'Gateway Status': True,
                'Last Check': receive_date,
            }

            # Retrieve data with this SIM Number from Bubble database (GatewayCheckWA)
            filter_params = [{"key": "Gateway ID", "constraint_type": "equals", "value": gateway_number}]
            filter_json = json.dumps(filter_params)
            params = {"constraints": filter_json}
            res = requests.get(f'{url_bubble}/GatewayCheckWA', headers=headers, params=params)
            data = res.json()
            data = data['response']['results'][0]
            # Forward data to Bubble database (Check Gateway)
            _id = data['_id']
            requests.patch(f'{url_bubble}/GatewayCheckWA/{_id}', headers=headers, data=payload_status)
            # Set WhatsApp status
            raw_wa_status = 'Check Gateway'
        
        else:
            error_type = 0

        # Payload (RAW WhatsApp)
        payload_raw = {
            'WA ID': id,
            'Receive Date': receive_date,
            'Sender': originator,
            'Gateway Port': port, 
            'Gateway ID': gateway_number,
            'Message': msg,
            'Error Type': error_type,
            'Status': raw_wa_status
        }

        # Forward data to Bubble database (Raw WhatsApp)
        requests.post(f'{url_bubble}/RAW_WhatsApp', headers=headers, data=payload_raw)



# ================================================================================================================
# Endpoint to check gateway status via SMS
@app.post("/check_gateway_status_sms")
async def check_gateway_status_sms(     
    gateway_1: Optional[str] = Form(None),
    gateway_2: Optional[str] = Form(None),
    gateway_3: Optional[str] = Form(None),
    gateway_4: Optional[str] = Form(None),
    gateway_5: Optional[str] = Form(None),
    gateway_6: Optional[str] = Form(None),
    gateway_7: Optional[str] = Form(None),
    gateway_8: Optional[str] = Form(None),
    gateway_9: Optional[str] = Form(None),
    gateway_10: Optional[str] = Form(None),
    gateway_11: Optional[str] = Form(None),
    gateway_12: Optional[str] = Form(None),
    gateway_13: Optional[str] = Form(None),
    gateway_14: Optional[str] = Form(None),
    gateway_15: Optional[str] = Form(None),
    gateway_16: Optional[str] = Form(None),
):

    numbers = [gateway_1, gateway_2, gateway_3, gateway_4, gateway_5, gateway_6, gateway_7, gateway_8, gateway_9, gateway_10, 
               gateway_11, gateway_12, gateway_13, gateway_14, gateway_15, gateway_16]

    # Sent trigger via SMS Masking
    for num in numbers:
        # if number is not empty
        if num:
            params = {
                "user": NUSA_USER_NAME,
                "password": NUSA_PASSWORD,
                "SMSText": 'the gateway is active',
                "GSM": num,
                "output": "json",
            }
            requests.get(url_send_sms, params=params)




# ================================================================================================================
# Endpoint to create N_Candidate json file
@app.post("/create_json_ncandidate")
async def create_json_ncandidate(
    event: str = Form(...),
    N_candidate: int = Form(...),
    ):
    event = event.lower()
    with open(f'{local_disk}/event_{event}.json', 'w') as json_file:
        json.dump({"n_candidate": N_candidate}, json_file)



# ================================================================================================================
# Endpoint to generate UID
@app.post("/getUID")
async def get_uid(
    event: str = Form(...),
    N_TPS: int = Form(...)
    ):
    event = event.lower()
    # Generate target file
    tools.create_target(event, N_TPS)
    
    # Forward file to Bubble database
    excel_file_path = f'{local_disk}/target_{event}.xlsx'
    
    def file_generator():
        with open(excel_file_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename=target_{event}.xlsx"

    # Return response
    return response



# ================================================================================================================
# Endpoint to generate SCTO xlsform
@app.post("/generate_xlsform")
async def generate_xlsform(
    form_title: str = Form(...),
    form_id: str = Form(...),
    target_file_name: str = Form(...),
    target_file: UploadFile = Form(...)
    ):

    event = target_file_name.split('_')[-1].split('.')[0].lower()

    # Save the target file to a temporary location
    with open(f'{local_disk}/{target_file_name}', 'wb') as target_file_content:
        target_file_content.write(target_file.file.read())

    # Get UIDs from the target file
    df = pd.read_excel(f'{local_disk}/{target_file_name}')

    # Rename regions
    df['Provinsi Ori'] = df['Provinsi'].copy()
    df['Kab/Kota Ori'] = df['Kab/Kota'].copy()
    df['Kecamatan Ori'] = df['Kecamatan'].copy()
    df['Kelurahan Ori'] = df['Kelurahan'].copy()
    for index, row in df.iterrows():
        input_regions = [row['Provinsi'], row['Kab/Kota'], row['Kecamatan'], row['Kelurahan']]
        output_regions = tools.rename_region(input_regions)
        df.loc[index, 'Provinsi'] = output_regions[0]
        df.loc[index, 'Kab/Kota'] = output_regions[1]
        df.loc[index, 'Kecamatan'] = output_regions[2]
        df.loc[index, 'Kelurahan'] = output_regions[3]

    # Save the target file after renaming regions
    df.to_excel(f'{local_disk}/{target_file_name}', index=False)

    # Break into batches
    n_batches = int(np.ceil(len(df) / 100))

    for batch in range(n_batches):
        start = batch * 100
        end = min((batch + 1) * 100, len(df)) - 1 
        tdf = df.loc[start:end, :]

        # Generate Text for API input
        data = '\n'.join([
            f'{{"UID": "{uid}", '
            f'"Active": false, '
            f'"Complete": false, '
            f'"SMS": false, '
            f'"SCTO": false, '
            f'"SMS Int": 0, '
            f'"SCTO Int": 0, '
            f'"Status": "Empty", '
            f'"Event ID": "{event}", '
            f'"Korprov": "{korprov}", '
            f'"Korwil": "{korwil}", '
            f'"Provinsi": "{provinsi}", '
            f'"Kab/Kota": "{kab_kota}", '
            f'"Kecamatan": "{kecamatan}", '
            f'"Kelurahan": "{kelurahan}", '
            f'"Provinsi Ori": "{provinsi_ori}", '
            f'"Kab/Kota Ori": "{kab_kota_ori}", '
            f'"Kecamatan Ori": "{kecamatan_ori}", '
            f'"Kelurahan Ori": "{kelurahan_ori}"}}'
            for uid, korprov, korwil, provinsi, kab_kota, kecamatan, kelurahan, provinsi_ori, kab_kota_ori, kecamatan_ori, kelurahan_ori in zip(
                tdf['UID'],
                tdf['Korprov'],
                tdf['Korwil'],
                tdf['Provinsi'],
                tdf['Kab/Kota'],
                tdf['Kecamatan'],
                tdf['Kelurahan'],
                tdf['Provinsi Ori'],
                tdf['Kab/Kota Ori'],
                tdf['Kecamatan Ori'],
                tdf['Kelurahan Ori']
            )
        ])

        # Populate votes table in bulk
        headers_populate_votes = {
            'Authorization': f'Bearer {BUBBLE_API_KEY}', 
            'Content-Type': 'text/plain'
            }
        requests.post(f'{url_bubble}/Votes/bulk', headers=headers_populate_votes, data=data)

        time.sleep(3)

    # Get UIDs and store as json
    uid_dict = {}
    for uid_start in range(1, len(df), 50):
        params = {'Event ID': event, 'start': uid_start, 'end': uid_start+50}
        res = requests.get(url_getUID, headers=headers, params=params)
        out = res.json()['response']
        uid_dict.update(zip(out['UID'], out['id_']))

    # filter_params = [{"key": "Event ID", "constraint_type": "equals", "value": event}]
    # filter_json = json.dumps(filter_params)
    # params = {"constraints": filter_json}
    # headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}
    # res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
    # uid_dict = {i['UID']:i['_id'] for i in res.json()['response']['results']}

    with open(f'{local_disk}/uid_{event}.json', 'w') as json_file:
        json.dump(uid_dict, json_file)

    # Generate xlsform logic using the target file
    tools.create_xlsform_template(f'{local_disk}/{target_file_name}', form_title, form_id, event)
    xlsform_path = f'{local_disk}/xlsform_{form_id}.xlsx'

    def file_generator():
        with open(xlsform_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename=xlsform_{form_id}.xlsx"

    return response



# ================================================================================================================
# Endpoint to delete event
@app.post("/delete_event")
async def delete_event(
    event: str = Form(...),
    form_id: str = Form(...)
    ):
    event = event.lower()
    os.system(f'rm -f {local_disk}/*_{event}.*')
    os.system(f'rm -f {local_disk}/*_{form_id}.*')



# ================================================================================================================
# Endpoint to trigger SCTO data processing
@app.post("/scto_data")
def scto_data(
    event: str = Form(...), 
    form_id: str = Form(...), 
    n_candidate: int = Form(...), 
    input_time: datetime = Form(...), 
    proc_id_a4: str = Form(None),
    ):

    #####################
    print(f'\nEvent: {event}\t Input Time: {input_time}')
    #####################

    try:

        # Calculate the oldest completion date based on the current time
        date_obj = input_time - timedelta(seconds=301)

        # Build SCTO connection
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)

        # Retrieve data from SCTO
        list_data = scto.get_form_data(form_id, format='json', shape='wide', oldest_completion_date=date_obj)

        # Loop over data
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(tools.scto_process, data, event, n_candidate, proc_id_a4)
    
    except Exception as e:
        print(f'Process: scto_data endpoint\t Keyword: {e}\n')



# ================================================================================================================
# Endpoint for regions aggregation
@app.post("/group_normalize")
async def region_aggregate(
    part_sum: list = Form(...), 
    total_sum: list = Form(...)
    ):
    part_sum = [int(value) for element in part_sum for value in element.split(",")]
    total_sum = [int(value) for element in total_sum for value in element.split(",")]
    result = list(np.round(np.array(part_sum) / np.array(total_sum) * 100, 2))
    return {"result": result}


# # ================================================================================================================
# # Run quick count (PILKADA) aggregator every 10 minutes

# def fetch_pilkada_quickcount():
#     while True:
#         try:
#             tools.fetch_pilkadaquickcount()
#         except Exception as e:
#             print(f"Error in fetch_quickcount: {str(e)}")
#         time.sleep(600)  # 600 seconds = 10 minutes


# # Create a thread for fetch_quickcount to run concurrently
# fetch_thread = threading.Thread(target=fetch_quickcount, daemon=True)
# fetch_thread.start()


# # ================================================================================================================
# # Run quick count (PILPRES) aggregator every 10 minutes

# def fetch_pilpres_quickcount():
#     while True:
#         try:
#             tools.fetch_pilpres_quickcount()
#         except Exception as e:
#             print(f"Error in fetch_quickcount: {str(e)}")
#         time.sleep(600)  # 600 seconds = 10 minutes


# # Create a thread for fetch_quickcount to run concurrently
# fetch_thread = threading.Thread(target=fetch_quickcount, daemon=True)
# fetch_thread.start()