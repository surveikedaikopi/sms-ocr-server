import warnings
warnings.filterwarnings("ignore", module="google.oauth2")

import os
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
                        total_votes = np.array(votes).astype(int).sum() + int(invalid)
                        summary = f'event:{event}\n' + '\n'.join([f'suara{i+1}:{votes[i]}' for i in range(number_candidates)]) + f'\nrusak:{invalid}' + f'\ntotal:{total_votes}\n'
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
                                if (np.array(votes) == np.array(data['SCTO Votes'])) & (int(invalid) == int(data['SCTO Invalid'])):
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
                                'SMS Gateway Number': gateway_number,
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
                            with open(f'uid_{event}.json', 'r') as json_file:
                                uid_dict = json.load(json_file)

                            # Forward data to Bubble database
                            _id = uid_dict[uid.upper()]
                            out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
                            print(out)

            except Exception as e:
                error_type = 1
                message = 'format tidak dikenali. kirim ulang dengan format yg sudah ditentukan. Contoh utk 3 paslon:\nkk#uid#event#01#02#03#rusak'
                print(f'Error Location: SMS - Error Type 1, keyword: {e}')

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
    N_TPS: int = Form(...)
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
    df.to_excel(target_file_name, index=False)

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
            df['UID'],
            df['Korprov'],
            df['Korwil'],
            df['Provinsi'],
            df['Kab/Kota'],
            df['Kecamatan'],
            df['Kelurahan'],
            df['Provinsi Ori'],
            df['Kab/Kota Ori'],
            df['Kecamatan Ori'],
            df['Kelurahan Ori']
        )
    ])

    # Populate votes table in bulk
    headers = {
        'Authorization': f'Bearer {BUBBLE_API_KEY}', 
        'Content-Type': 'text/plain'
        }
    requests.post(f'{url_bubble}/Votes/bulk', headers=headers, data=data)

    # Get UIDs and store as json
    filter_params = [{"key": "Event ID", "constraint_type": "text contains", "value": event}]
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
# Endpoint to trigger SCTO data processing
@app.post("/scto_data")
def scto_data(
    event: str = Form(...), 
    form_id: str = Form(...), 
    n_candidate: int = Form(...), 
    input_time: datetime = Form(...), 
    processor_id_a4: str = Form(None),
    processor_id_plano: str = Form(None)
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
                    executor.submit(tools.scto_process, data, event, n_candidate, processor_id_a4, processor_id_plano)
    
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