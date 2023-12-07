# Filter out the Python 3.7 deprecation warning from google.oauth2
import warnings
warnings.filterwarnings("ignore", module="google.oauth2")

import os
import time
import json
import tools
import requests
import numpy as np
import pandas as pd
import concurrent.futures
from fastapi import Request
from dotenv import load_dotenv
from pysurveycto import SurveyCTOObject
from datetime import datetime, timedelta
from fastapi import Form, FastAPI, UploadFile
from fastapi.responses import StreamingResponse


# ================================================================================================================
# Initial Setup

# Load env
load_dotenv()

# Define app
app = FastAPI()

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
# Endpoint to read the "inbox.txt" file
@app.get("/read")
async def read_inbox():
    try:
        with open("inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"inbox_data": data}
    except FileNotFoundError:
        return {"message": "File not found"}



# ================================================================================================================
# Endpoint to receive SMS message, to validate, and to forward the pre-processed data

# Define the number of endpoints
num_endpoints = 16

# Endpoint to receive SMS message, to validate, and to forward the pre-processed data
for port in range(1, num_endpoints + 1):
    @app.post(f"/receive-{port}")
    async def receive_sms(
        request: Request,
        id: int = Form(...),
        gateway_number: int = Form(...),
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
            "Gateway Number": gateway_number,
            "Sender": originator,
            "Message": msg,
            "Receive Date": receive_date
        }

        # Log the received data to a JSON file
        with open("inbox.json", "a") as json_file:
            json.dump(raw_data, json_file)
            json_file.write('\n')  # Add a newline to separate the JSON objects

        # Split message
        info = msg.lower().split('#')

        # Default Values
        error_type = None
        raw_sms_status = 'Rejected'

        # Check Error Type 1 (prefix)
        if info[0] == 'kk':

            try:
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
                        total_votes = np.array(votes).astype(int).sum()
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
                            validator = data['response']['results'][0]['Validator']

                            # If SCTO data exists, check if they are consistent
                            if scto:
                                if votes == data['response']['results'][0]['SCTO Votes']:
                                    status = 'Verified'
                                    validator = 'System'
                                else:
                                    status = 'Not Verified'
                                    note = 'SMS vs SCTO not consistent'
                            else:
                                status = 'SMS Only'
                            
                            # Extract the hour as an integer
                            tmp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                            hour = tmp.hour

                            # if note is not yet defined
                            try:
                                note
                            except:
                                note = ''
                            
                            # Delta Time
                            scto_timestamp = data['response']['results'][0]['SCTO Timestamp']
                            if scto_timestamp:
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
                                'SMS Gateway Port': port,
                                'SMS Gateway Number': gateway_number,
                                'SMS Sender': originator,
                                'SMS Timestamp': receive_date,
                                'SMS Hour': hour,
                                'Event Name': event,
                                'SMS Votes': votes,
                                'SMS Invalid': invalid,
                                'SMS Total Voters': total_votes, 
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
                                'Note': note,
                                'Validator': validator
                            }

                            raw_sms_status = 'Accepted'

                            # Load the JSON file into a dictionary
                            with open(f'uid_{event}.json', 'r') as json_file:
                                uid_dict = json.load(json_file)

                            # Forward data to Bubble database
                            _id = uid_dict[uid.upper()]
                            requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)

            except:
                error_type = 1
                message = 'format tidak dikenali. kirim ulang dengan format yg sudah ditentukan. Contoh utk 3 paslon:\nkk#uid#event#01#02#03#rusak'

            # # Return the message to the sender via SMS Gateway
            # params = {
            #     "user": "taufikadinugraha_api",
            #     "password": "SekarangSeriusSMS@ku99",
            #     "SMSText": message,
            #     "GSM": originator,
            #     "output": "json",
            # }
            # requests.get(url_send_sms, params=params)

        else:
            error_type = 0

        # Payload (RAW SMS)
        payload_raw = {
            'SMS ID': id,
            'Receive Date': receive_date,
            'Sender': originator,
            'Gateway Port': port, 
            'Gateway Number': gateway_number,
            'Message': msg,
            'Error Type': error_type,
            'Status': raw_sms_status
        }

        # Forward data to Bubble database (Raw SMS)
        requests.post(f'{url_bubble}/RAW_SMS', headers=headers, data=payload_raw)



# ================================================================================================================
# Endpoint to create N_Candidate json file
@app.post("/create_json_ncandidate")
async def create_json_ncandidate(
    event: str = Form(...),
    N_candidate: int = Form(...),
):
    with open(f'event_{event}.json', 'w') as json_file:
        json.dump({"n_candidate": N_candidate}, json_file)



# ================================================================================================================
# Endpoint to generate UID
@app.post("/getUID")
async def get_uid(
    event: str = Form(...),
    N_TPS: int = Form(...),
):

    # Generate target file
    tools.create_target(event, N_TPS)
    
    # Forward file to Bubble database
    excel_file_path = f'target_{event}.xlsx'
    
    def file_generator():
        with open(excel_file_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename={excel_file_path}"

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

    event = target_file_name.split('_')[-1].split('.')[0]

    # Save the target file to a temporary location
    with open(target_file_name, 'wb') as target_file_content:
        target_file_content.write(target_file.file.read())

    # Get UIDs from the target file
    df = pd.read_excel(target_file_name)

    # Generate Text for API input
    data = '\n'.join([
        f'{{"UID": "{uid}", '
        f'"Active": false, '
        f'"Complete": false, '
        f'"SMS": false, '
        f'"SCTO": false, '
        f'"Status": "Empty", '
        f'"Event Name": "{event}", '
        f'"Korwil": "{korwil}", '
        f'"Provinsi": "{provinsi}", '
        f'"Kab/Kota": "{kab_kota}", '
        f'"Kecamatan": "{kecamatan}", '
        f'"Kelurahan": "{kelurahan}"}}'
        for uid, korwil, provinsi, kab_kota, kecamatan, kelurahan in zip(
            df['UID'],
            df['Korwil'],
            df['Provinsi'],
            df['Kab/Kota'],
            df['Kecamatan'],
            df['Kelurahan']
        )
    ])

    # Populate votes table in bulk
    headers = {
        'Authorization': f'Bearer {BUBBLE_API_KEY}', 
        'Content-Type': 'text/plain'
        }
    requests.post(f'{url_bubble}/Votes/bulk', headers=headers, data=data)

    # Get UIDs and store as json
    filter_params = [{"key": "Event Name", "constraint_type": "text contains", "value": event}]
    filter_json = json.dumps(filter_params)
    params = {"constraints": filter_json}
    headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}
    res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
    uid_dict = {i['UID']:i['_id'] for i in res.json()['response']['results']}
    with open(f'uid_{event}.json', 'w') as json_file:
        json.dump(uid_dict, json_file)

    # Generate xlsform logic using the target file
    tools.create_xlsform_template(target_file_name, form_title, form_id, event)
    xlsform_path = f'xlsform_{form_id}.xlsx'

    def file_generator():
        with open(xlsform_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename={xlsform_path}"

    return response



# ================================================================================================================
# Endpoint to delete event
@app.post("/delete_event")
async def delete_event(
    event: str = Form(...),
    form_id: str = Form(...)
):
    os.system(f'rm -f *_{event}.*')
    os.system(f'rm -f *_{form_id}.*')



# ================================================================================================================
# Process SCTO data

def scto_process(data, event, n_candidate, processor_id):

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



# ================================================================================================================
# Asynchronous process
def process_data(event, form_id, n_candidate, date_obj, processor_id):
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



# ================================================================================================================
# Running All The Time

# Time period
minute_delta = 10

while True:

    # Get the current time in the server's time zone
    current_time_server = tools.convert_to_server_timezone(datetime.now())

    print(datetime.now())

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
