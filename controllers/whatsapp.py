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
            uid, event = info[1].lower(), info[2].lower()
            with open(f'{local_disk}/event_{event}.json', 'r') as json_file:
                number_candidates = json.load(json_file)['n_candidate']

            format = 'KK#UID#EventID#' + '#'.join([f'0{i+1}' for i in range(number_candidates)]) + '#Rusak'
            template_error_msg = 'cek & kirim ulang dgn format:\n' + format

            tmp = pd.read_excel(f'{local_disk}/target_{event}.xlsx', usecols=['UID'])

            # Check Error Type 2 (UID within the context of EventID)
            if uid not in tmp['UID'].str.lower().tolist():
                message = f'UID "{uid.upper()}" tidak terdaftar untuk EventID "{event}", ' + template_error_msg
                error_type = 2
            elif len(info) != number_candidates + 4:
                message = 'Data tidak lengkap, ' + template_error_msg
                error_type = 3
            else:
                votes = np.array(info[3:-1]).astype(int)
                invalid = int(info[-1])
                total_votes = votes.sum() + invalid
                summary = f'EventID: {event}\n' + '\n'.join([f'Paslon_{i+1}: {votes[i]}' for i in range(number_candidates)]) + f'\nTidak Sah: {invalid}\nTotal: {total_votes}\n'

                if total_votes > 700:
                    message = summary + 'Jumlah suara melebihi 700, ' + template_error_msg
                    error_type = 4
                else:
                    message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama:\n' + format
                    filter_params = [
                        {"key": "UID", "constraint_type": "equals", "value": uid.upper()},
                        {"key": "Event ID", "constraint_type": "equals", "value": event}
                    ]
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

                    raw_wa_status = 'Accepted'
                    with open(f'{local_disk}/uid_{event}.json', 'r') as json_file:
                        uid_dict = json.load(json_file)
                    requests.patch(f'{url_bubble}/votes/{uid_dict[uid.upper()]}', headers=headers, data=payload)

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
        print(PAYLOADS)
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