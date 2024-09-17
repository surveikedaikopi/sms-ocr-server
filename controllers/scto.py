import json
import requests
import numpy as np
from fastapi import Form
from datetime import datetime, timedelta
from pysurveycto import SurveyCTOObject
from concurrent.futures import ThreadPoolExecutor

from utils.utils import *
from config.config import *
from utils.preprocess import *






# Function to process SCTO data
def scto_data(
    event: str = Form(...),
    form_id: str = Form(...), 
    n_candidate: int = Form(...), 
    input_time: datetime = Form(...), 
    proc_id_a4: str = Form(None),
):
    print(f'\nEvent: {event}\t Input Time: {input_time}')

    try:
        # Calculate the oldest completion date based on the current time
        date_obj = input_time - timedelta(seconds=301)

        # Build SCTO connection
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)

        # Retrieve data from SCTO
        list_data = scto.get_form_data(form_id, format='json', shape='wide', oldest_completion_date=date_obj)

        # Loop over data
        if len(list_data) > 0:
            with ThreadPoolExecutor() as executor:
                for data in list_data:
                    # Run 'scto_process' function asynchronously
                    executor.submit(scto_process, data, event, n_candidate, proc_id_a4)
    
    except Exception as e:
        print(f'Process: scto_data endpoint\t Keyword: {e}\n')






def scto_process(data, event, n_candidate, proc_id_a4):
    """
    Process SCTO data and update the Bubble server.
    """
    event = event.lower()
    try:
        uid = data['UID']
        std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p") + timedelta(hours=7)
        filter_params = [
            {"key": "UID", "constraint_type": "equals", "value": uid},
            {"key": "Event ID", "constraint_type": "equals", "value": event}
        ]
        res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params={"constraints": json.dumps(filter_params)})
        data_bubble = res_bubble.json()['response']['results'][0]
        
        validator = data_bubble.get('Validator')
        sms_timestamp = data_bubble.get('SMS Timestamp')
        delta_time_hours = None
        if sms_timestamp:
            delta_time = abs(std_datetime - datetime.strptime(sms_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"))
            delta_time_hours = delta_time.total_seconds() / 3600
        
        coordinate = np.array(data['koordinat'].split(' ')[1::-1]).astype(float)
        loc = get_location(coordinate)
        key = data['KEY'].split('uuid:')[-1]
        link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"
        formulir_c1_a4 = data['formulir_c1_a4']
        formulir_c1_plano = data['formulir_c1_plano']
        selfie = data['selfie']

        if proc_id_a4:
            try:
                attachment_url = data['formulir_c1_a4']
                scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)
                deviceid = data['deviceid']
                ai_votes, ai_invalid = read_form(scto, attachment_url, n_candidate, proc_id_a4, deviceid)
            except Exception as e:
                print(f'Process: scto_process endpoint\t Keyword: {e}\n')
                ai_votes = [0] * n_candidate
                ai_invalid = 0
        else:
            ai_votes = [0] * n_candidate
            ai_invalid = 0
        
        sms = data_bubble['SMS']
        status = 'Not Verified' if sms else 'SCTO Only'
        
        gps_status = 'Verified' if all(data_bubble[k] == loc[k] for k in ['Provinsi', 'Kab/Kota', 'Kecamatan', 'Kelurahan']) else 'Not Verified'
        
        payload = {
            'Active': True,
            'Complete': sms,
            'UID': uid,
            'Event ID': event,
            'SCTO TPS': data['no_tps'],
            'SCTO Dapil': data['dapil'],
            'SCTO Address': data['alamat'],
            'SCTO RT': data['rt'],
            'SCTO RW': data['rw'],
            'SCTO': True,
            'SCTO Int': 1,
            'SCTO Enum Name': data['nama'],
            'SCTO Enum Phone': data['no_hp'],
            'SCTO Timestamp': std_datetime,
            'SCTO Hour': std_datetime.hour,
            'SCTO Provinsi': data['selected_provinsi'].replace('_', ' '),
            'SCTO Kab/Kota': data['selected_kabkota'].replace('_', ' '),
            'SCTO Kecamatan': data['selected_kecamatan'].replace('_', ' '),
            'SCTO Kelurahan': data['selected_kelurahan'].replace('_', ' '),
            'SCTO Votes': ai_votes,
            'SCTO Invalid': ai_invalid,
            'SCTO C1 A4': formulir_c1_a4,
            'SCTO C1 Plano': formulir_c1_plano,
            'SCTO Selfie': selfie,
            'GPS Provinsi': loc['Provinsi'],
            'GPS Kab/Kota': loc['Kab/Kota'],
            'GPS Kecamatan': loc['Kecamatan'],
            'GPS Kelurahan': loc['Kelurahan'],
            'GPS Status': gps_status,
            'Delta Time': delta_time_hours,
            'Status': status,
            'Survey Link': link,
            'Validator': validator
        }
        
        with open(f'{local_disk}/uid_{event}.json', 'r') as json_file:
            uid_dict = json.load(json_file)
        _id = uid_dict[uid.upper()]
        out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
        print(out)
    except Exception as e:
        with print_lock:
            print(f'Process: scto_process\t Keyword: {e}')