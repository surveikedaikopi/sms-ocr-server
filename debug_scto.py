import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
from pysurveycto import SurveyCTOObject

SCTO_SERVER_NAME='risetkedaikopi'
SCTO_USER_NAME='surveikedaikopi@gmail.com'
SCTO_PASSWORD='Kedai_k10'
scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)
form_id = 'simulasi_hitungcepat'
url_bubble='https://quick-count.bubbleapps.io/version-test/api/1.1/obj'
headers = {'Authorization': 'Bearer cecd6c1aa78871f6746b03bb1997508f'}

processor_id = None
n_candidate = 3
event = 'pilpres'

receive_date = '2023-12-11 11:00:00'
date_obj = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")


list_data = scto.get_form_data(form_id, format='json', shape='wide', oldest_completion_date=date_obj)
data = list_data[-1]




# UID
uid = data['UID']

uid = 'DXB'

# SCTO Timestamp
std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p")
std_datetime = std_datetime + timedelta(hours=7)

# Retrieve data with this UID from Bubble database
filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid}]
filter_json = json.dumps(filter_params)
params = {"constraints": filter_json}
res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
data_bubble = res_bubble.json()
data_bubble = data_bubble['response']['results'][0]

# Delta Time
if 'SMS Timestamp' in data_bubble:
    sms_timestamp = datetime.strptime(data_bubble['SMS Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
    delta_time = abs(std_datetime - sms_timestamp)
    delta_time_hours = delta_time.total_seconds() / 3600
else:
    delta_time_hours = None

# GPS location
coordinate = np.array(data['koordinat'].split(' ')[1::-1]).astype(float)

# Survey Link
key = data['KEY'].split('uuid:')[-1]
link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"

# OCR C1-Form
if processor_id:
    try:
        attachment_url = data['foto_jumlah_suara']
        # Build SCTO connection
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)
        ai_votes, ai_invalid = read_form(scto, attachment_url, n_candidate, processor_id)
    except:
        ai_votes = [0] * n_candidate
        ai_invalid = 0            
else:
    ai_votes = [0] * n_candidate
    ai_invalid = 0

# Check if SMS data exists
sms = data_bubble['SMS']

# If SMS data exists, check if they are consistent
if sms:
    if ai_votes == data_bubble['SMS Votes']:
        status = 'Verified'
    else:
        status = 'Not Verified'
else:
    status = 'SCTO Only'

# Update GPS status
gps_status = 'Not Verified'

# Payload
payload = {
    'Active': True,
    'Complete': sms,
    'UID': uid,
    'SCTO TPS': data['no_tps'],
    'SCTO Address': data['alamat'],
    'SCTO RT': data['rt'],
    'SCTO RW': data['rw'],
    'SCTO': True,
    'SCTO Int': 1,
    'SCTO Enum Name': data['nama'],
    'SCTO Enum Phone': data['no_hp'],
    'SCTO Timestamp': std_datetime,
    'SCTO Hour': std_datetime.hour,
    'SCTO Provinsi': data['selected_provinsi'].replace('-', ' '),
    'SCTO Kab/Kota': data['selected_kabkota'].replace('-', ' '),
    'SCTO Kecamatan': data['selected_kecamatan'].replace('-', ' '),
    'SCTO Kelurahan': data['selected_kelurahan'].replace('-', ' '),
    'SCTO Votes': ai_votes,
    'SCTO Invalid': ai_invalid,
    'GPS Status': gps_status,
    'Delta Time': delta_time_hours,
    'Status': status,
    'Survey Link': link
}

# Forward data to Bubble Votes database
_id = '1702349496161x182039199907024320'
out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
print(out)
