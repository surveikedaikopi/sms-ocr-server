import os
import time
import json
import tools
import requests
import threading
import numpy as np
import concurrent.futures
from dotenv import load_dotenv
from pysurveycto import SurveyCTOObject
from datetime import datetime, timedelta


# ================================================================================================================
# Initial Setup

# Load env
load_dotenv()

# Global Variables
url_send_sms = os.environ.get('url_send_sms')
url_bubble = os.environ.get('url_bubble')
BUBBLE_API_KEY = os.environ.get('BUBBLE_API_KEY')
SCTO_SERVER_NAME = os.environ.get('SCTO_SERVER_NAME')
SCTO_USER_NAME = os.environ.get('SCTO_USER_NAME')
SCTO_PASSWORD = os.environ.get('SCTO_PASSWORD')

# Bubble Headers
headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}



# ================================================================================================================
# Process SCTO data

def scto_process(data, event, n_candidate, processor_id):

    try:

        # UID
        uid = data['UID']

        # SCTO Timestamp
        std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p")
        std_datetime = std_datetime + timedelta(hours=7)
        
        # Delta Time
        if data['SMS Timestamp']:
            sms_timestamp = datetime.strptime(data['SMS Timestamp'], "%Y-%m-%d %H:%M:%S")
            delta_time = abs(std_datetime - sms_timestamp)
            delta_time_hours = delta_time.total_seconds() / 3600
        else:
            delta_time_hours = None

        # GPS location
        coordinate = np.array(data['koordinat'].split(' ')[1::-1]).astype(float)
        loc = tools.get_location(coordinate)
        
        # Survey Link
        key = data['KEY'].split('uuid:')[-1]
        link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"
        
        # OCR C1-Form
        if processor_id:
            try:
                attachment_url = data['foto_jumlah_suara']
                # Build SCTO connection
                scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)
                ai_votes, ai_invalid = tools.read_form(scto, attachment_url, n_candidate, processor_id)
            except:
                ai_votes = [0] * n_candidate
                ai_invalid = 0            
        else:
            ai_votes = [0] * n_candidate
            ai_invalid = 0

        # Retrieve data with this UID from Bubble database
        filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid}]
        filter_json = json.dumps(filter_params)
        params = {"constraints": filter_json}
        res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
        data_bubble = res_bubble.json()

        # Check if SMS data exists
        sms = data_bubble['response']['results'][0]['SMS']

        # If SMS data exists, check if they are consistent
        if sms:
            if ai_votes == data_bubble['response']['results'][0]['SMS Votes']:
                status = 'Verified'
            else:
                status = 'Not Verified'
                note = 'SMS vs SCTO not consistent'
        else:
            status = 'SCTO Only'

        # if note is not yet defined
        try:
            note
        except:
            note = ''

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
            'SCTO Enum Name': data['nama'],
            'SCTO Enum Phone': data['no. hp'],
            'SCTO Timestamp': std_datetime,
            'SCTO Hour': std_datetime.hour,
            'SCTO Provinsi': data['selected_provinsi'].replace('-', ' '),
            'SCTO Kab/Kota': data['selected_kabkota'].replace('-', ' '),
            'SCTO Kecamatan': data['selected_kecamatan'].replace('-', ' '),
            'SCTO Kelurahan': data['selected_kelurahan'].replace('-', ' '),
            'SCTO Votes': ai_votes,
            'SCTO Invalid': ai_invalid,
            'SCTO Total Voters': np.sum(ai_votes) + ai_invalid,
            'GPS Provinsi': loc['Provinsi'],
            'GPS Kab/Kota': loc['Kab/Kota'],
            'GPS Kecamatan': loc['Kecamatan'],
            'GPS Kelurahan': loc['Kelurahan'],
            'Delta Time': delta_time_hours,
            'Status': status,
            'Survey Link': link
        }

        # Load the JSON file into a dictionary
        with open(f'uid_{event}.json', 'r') as json_file:
            uid_dict = json.load(json_file)

        # Forward data to Bubble Votes database
        _id = uid_dict[uid.upper()]
        requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)

    except Exception as e:
        # Handle the exception (you can log it, print an error message, etc.)
        with print_lock:
            print(f"Error processing data: {e}")



# ================================================================================================================
# Asynchronous process
def process_data(event, form_id, n_candidate, date_obj, processor_id):
    try:

        # Build SCTO connection
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)

        # Retrieve data from SCTO
        list_data = scto.get_form_data(form_id, format='json', shape='wide', oldest_completion_date=date_obj)

        # Loop over data
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(scto_process, data, event, n_candidate, processor_id)
    
    except Exception as e:
        # Handle the exception (you can log it, print an error message, etc.)
        with print_lock:
            print(f"Error processing data: {e}")



# ================================================================================================================
# Create a threading lock for synchronization
print_lock = threading.Lock()



# ================================================================================================================
# Scheduler

# Time period
minute_delta = 10

while True:

    # Get the current time in the server's time zone
    current_time_server = tools.convert_to_server_timezone(datetime.now())

    # Calculate the oldest completion date based on the current time
    date_obj = current_time_server - timedelta(minutes=minute_delta)

    # Retrieve events from Bubble database
    res = requests.get(f'{url_bubble}/Events', headers=headers)
    data = res.json()

    if data['response']['count'] > 0:

        events = [i['Event Name'] for i in data['response']['results']]
        form_ids = [i['SCTO FormID'] for i in data['response']['results']]
        n_candidates = [i['Number of Candidates'] for i in data['response']['results']]
        processor_ids = [i['OCR Processor ID'] for i in data['response']['results']]

        # Process data asynchronously
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_data, events, form_ids, n_candidates, [date_obj]*len(events), processor_ids)

    # Wait for 10 minutes before the next iteration
    time.sleep(minute_delta * 60) 