import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime


url_bubble = 'https://quick-count.bubbleapps.io/version-test/api/1.1/obj'
headers = {'Authorization': 'Bearer cecd6c1aa78871f6746b03bb1997508f'}

msg = 'kk# #pilpres#1#2#3#0'
receive_date = '2023-12-13 07:00:00'

# Split message
info = msg.lower().split('#')

uid = info[1].lower()
event = info[2]

# Get number of candidate pairs
with open(f'event_{event}.json', 'r') as json_file:
    json_content = json.load(json_file)
    number_candidates = json_content['n_candidate']

format = 'kk#uid#event#' + '#'.join([f'0{i+1}' for i in range(number_candidates)]) + '#rusak'
template_error_msg = 'cek & kirim ulang dgn format:\n' + format

tmp = pd.read_excel(f'target_{event}.xlsx', usecols=['UID'])

# Check Error Type 2 (UID)
if uid not in tmp['UID'].str.lower().tolist():
    message = f'Unique ID (UID) "{uid.upper()}" tidak terdaftar, ' + template_error_msg
    error_type = 2
else:
    # Check Error Type 3 (data completeness)
    if len(info) != number_candidates + 4:
        message = 'Data tidak sesuai, ' + template_error_msg
        error_type = 3
    else:
        # Get votes
        votes = info[3:-1]
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
        summary = f'event:{event}\n' + '\n'.join([f'0{i+1}:{votes[i]}' for i in range(number_candidates)]) + f'\nrusak:{invalid}' + f'\ntotal:{total_votes}\n'
        # Check Error Type 4 (maximum votes)
        if total_votes > 300:
            message = summary + 'Jumlah suara melebihi 300, ' + template_error_msg
            error_type = 4
        else:
            message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama:\n' + format

            # Retrieve data with this UID from Bubble database
            filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid.upper()}]
            filter_json = json.dumps(filter_params)
            params = {"constraints": filter_json}
            res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
            data = res.json()

            # Check if SCTO data exists
            scto = data['response']['results'][0]['SCTO']

            # Get existing validator
            if 'validator' in data:
                validator = data['response']['results'][0]['Validator']
            else:
                validator = None

            # If SCTO data exists, check if they are consistent
            if scto:
                if votes == data['response']['results'][0]['SCTO Votes']:
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
                scto_timestamp = data['response']['results'][0]['SCTO Timestamp']
                sms_timestamp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                scto_timestamp = datetime.strptime(scto_timestamp, "%Y-%m-%d %H:%M:%S")
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
                'SMS Gateway Port': 1,
                'SMS Gateway Number': 1234,
                'SMS Sender': 5678,
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
                'Final Votes': votes,
                'Invalid Votes': invalid,
                'Complete': scto,
                'Status': status,
                'Delta Time': delta_time_hours,
                'Validator': validator
            }

            raw_sms_status = 'Accepted'

            # Load the JSON file into a dictionary
            with open(f'uid_{event}.json', 'r') as json_file:
                uid_dict = json.load(json_file)

            # Forward data to Bubble database
            _id = uid_dict[uid.upper()]
            out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
            print(out)