import json
import requests
import numpy as np
import pandas as pd
from fastapi import Form, Request
from datetime import datetime
from config.config import *

async def receive_whatsapp(
    request: Request,
    id: str = Form(...),
    gateway_number: str = Form(...),
    originator: str = Form(...),
    msg: str = Form(...),
    receive_date: str = Form(...),
):
    """
    Handles incoming WhatsApp messages, processes the message, checks for errors,
    logs the data, and forwards it to a Bubble database.

    Parameters:
    - request: The HTTP request object.
    - id: The ID of the WhatsApp message.
    - gateway_number: The gateway number from which the message was received.
    - originator: The sender of the message.
    - msg: The content of the message.
    - receive_date: The date and time the message was received.
    """
    
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
                    vote3 = votes[2] if len(votes) > 2 else None
                    vote4 = votes[3] if len(votes) > 3 else None
                    vote5 = votes[4] if len(votes) > 4 else None
                    vote6 = votes[5] if len(votes) > 5 else None
                    # Get invalid votes
                    invalid = info[-1]
                    # Get total votes
                    total_votes = np.array(votes).astype(int).sum() + int(invalid)
                    summary = f'EventID: {event}\n' + '\n'.join([f'Paslon0{i+1}: {votes[i]}' for i in range(number_candidates)]) + f'\nTidak Sah: {invalid}' + f'\nTotal: {total_votes}\n'

                    # Check Error Type 4 (maximum votes)
                    if total_votes > 700:
                        message = summary + 'Jumlah suara melebihi 700, ' + template_error_msg
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
                        validator = data.get('Validator', None)

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
                        requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, json=payload)

        except Exception as e:
            error_type = 1
            message = 'Format tidak dikenali. Kirim ulang dengan format yg sudah ditentukan. Contoh utk 3 paslon:\nKK#UID#EventID#01#02#03#Rusak'
            print(f'Error Location: WhatsApp - Error Type 1, keyword: {e}')

        # Return the message to the sender via WhatsApp Gateway
        HEADERS = {
            "Accept": "application/json",
            "APIKey": NUSA_API_KEY
        }
        PAYLOADS = {
            'message': message,
            'destination': originator,
            'sender': list_WhatsApp_Gateway[int(port)],
            'include_unsubscribe': False
        }
        requests.post(url_send_wa, headers=HEADERS, json=PAYLOADS)

    elif msg == 'the gateway is active':
        """
        Handles the 'gateway is active' message, updates the gateway status in the Bubble database.
        """
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
        requests.patch(f'{url_bubble}/GatewayCheckWA/{_id}', headers=headers, json=payload_status)
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
    requests.post(f'{url_bubble}/RAW_WhatsApp', headers=headers, json=payload_raw)