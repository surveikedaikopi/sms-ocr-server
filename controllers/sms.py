import json
import requests
import numpy as np
import pandas as pd
from fastapi import Form, Request
from datetime import datetime
from typing import Optional
from config.config import local_disk, url_send_sms, url_bubble, NUSA_USER_NAME, NUSA_PASSWORD, headers

async def receive_sms(
    request: Request,
    id: str = Form(...),
    gateway_number: str = Form(...),
    originator: str = Form(...),
    msg: str = Form(...),
    receive_date: str = Form(...),
):
    """
    Receives an SMS message, validates it, and forwards the pre-processed data to the Bubble database.

    Parameters:
    - request: The HTTP request object.
    - id: The ID of the SMS message.
    - gateway_number: The gateway number from which the SMS was received.
    - originator: The sender of the SMS.
    - msg: The content of the SMS message.
    - receive_date: The date and time when the SMS was received.

    The function performs the following steps:
    1. Extracts the port number from the request URL.
    2. Logs the received data to a JSON file.
    3. Splits the message content and removes spaces.
    4. Validates the message format and content.
    5. Checks for various error types and constructs appropriate responses.
    6. Forwards the validated data to the Bubble database.
    7. Sends a response message back to the sender via SMS.
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
            uid, event = info[1].lower(), info[2].lower()
            with open(f'{local_disk}/event_{event}.json', 'r') as json_file:
                number_candidates = json.load(json_file)['n_candidate']

            format = 'KK#UID#EventID#' + '#'.join([f'0{i+1}' for i in range(number_candidates)]) + '#Rusak'
            template_error_msg = 'cek & kirim ulang dgn format:\n' + format

            tmp = pd.read_excel(f'{local_disk}/target_{event}.xlsx', usecols=['UID'])

            # Check Error Type 2 (UID)
            if uid not in tmp['UID'].str.lower().tolist():
                message = f'UID "{uid.upper()}" tidak terdaftar, ' + template_error_msg
                error_type = 2
            elif len(info) != number_candidates + 4:
                message = 'Data tidak lengkap, ' + template_error_msg
                error_type = 3
            else:
                votes = np.array(info[3:-1]).astype(int)
                invalid = int(info[-1])
                total_votes = votes.sum() + invalid
                summary = f'EventID: {event}\n' + '\n'.join([f'Paslon0{i+1}: {votes[i]}' for i in range(number_candidates)]) + f'\nTidak Sah: {invalid}\nTotal: {total_votes}\n'

                if total_votes > 700:
                    message = summary + 'Jumlah suara melebihi 700, ' + template_error_msg
                    error_type = 4
                else:
                    message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama:\n' + format
                    filter_params = [{"key": "UID", "constraint_type": "equals", "value": uid.upper()}]
                    res = requests.get(f'{url_bubble}/Votes', headers=headers, params={"constraints": json.dumps(filter_params)})
                    data = res.json()['response']['results'][0]

                    validator = data.get('Validator', None)
                    scto = data['SCTO']
                    status = 'Verified' if scto and np.array_equal(votes, np.array(data['SCTO Votes']).astype(int)) and invalid == int(data['SCTO Invalid']) else 'Not Verified' if scto else 'SMS Only'
                    hour = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S").hour
                    delta_time_hours = (datetime.strptime(data['SCTO Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")).total_seconds() / 3600 if 'SCTO Timestamp' in data else None

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
                        'SMS Votes': votes.tolist(),
                        'SMS Invalid': invalid,
                        'Vote1': votes[0] if len(votes) > 0 else None,
                        'Vote2': votes[1] if len(votes) > 1 else None,
                        'Vote3': votes[2] if len(votes) > 2 else None,
                        'Vote4': votes[3] if len(votes) > 3 else None,
                        'Vote5': votes[4] if len(votes) > 4 else None,
                        'Vote6': votes[5] if len(votes) > 5 else None,
                        'Total Votes': total_votes,
                        'Final Votes': votes.tolist(),
                        'Invalid Votes': invalid,
                        'Complete': scto,
                        'Status': status,
                        'Delta Time': delta_time_hours,
                        'Validator': validator
                    }

                    raw_sms_status = 'Accepted'
                    with open(f'{local_disk}/uid_{event}.json', 'r') as json_file:
                        uid_dict = json.load(json_file)
                    requests.patch(f'{url_bubble}/votes/{uid_dict[uid.upper()]}', headers=headers, data=payload)

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
        """
        Handles the special case where the message indicates that the gateway is active.

        The function performs the following steps:
        1. Constructs a payload to update the gateway status.
        2. Retrieves the current gateway status from the Bubble database.
        3. Updates the gateway status in the Bubble database.
        4. Sets the SMS status to 'Check Gateway'.
        """
        # Payload (Gateway Check)
        payload_status = {
            'Gateway Port': port,
            'Gateway Status': True,
            'Last Check': receive_date,
        }

        # Retrieve data with this SIM Number from Bubble database (GatewayCheckSMS)
        filter_params = [{"key": "Gateway ID", "constraint_type": "equals", "value": gateway_number}]
        res = requests.get(f'{url_bubble}/GatewayCheckSMS', headers=headers, params={"constraints": json.dumps(filter_params)})
        data = res.json()['response']['results'][0]
        requests.patch(f'{url_bubble}/GatewayCheckSMS/{data["_id"]}', headers=headers, data=payload_status)
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
    """
    Sends a trigger message to check the status of multiple gateways.

    Parameters:
    - gateway_1 to gateway_16: Optional gateway numbers to check.

    The function performs the following steps:
    1. Constructs a list of gateway numbers.
    2. Iterates over the list and sends a trigger message to each non-empty gateway number.
    """
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