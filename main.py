import os
import json
import tools
import requests
import numpy as np
import pandas as pd
from fastapi import Request
from datetime import datetime
from fastapi import Form, FastAPI, UploadFile
from fastapi.responses import StreamingResponse

app = FastAPI()

url_send_sms = "https://api.nusasms.com/api/v3/sendsms/plain"
url_bubble = "https://quick-count.bubbleapps.io/version-test/api/1.1/obj"
API_KEY = "cecd6c1aa78871f6746b03bb1997508f"
headers = {'Authorization': f'Bearer {API_KEY}'}

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

        # Default Error Type & Status
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
                        votes = info[3:]
                        invalid = info[-1]
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

                            # If SCTO data exists, check if they are consistent
                            if scto:
                                if votes[:-1] == data['response']['results'][0]['SCTO Votes']:
                                    status = 'Verified'
                                else:
                                    status = 'Not Verified'
                                    note = 'SMS vs SCTO not consistent'
                            else:
                                status = 'SMS Only'

                            # Convert receive_date_str to datetime format
                            tmp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                            # Extract the hour as an integer
                            hour = tmp.hour

                            # Payload
                            payload = {
                                'Active': True,
                                'SMS': True,
                                'UID': uid.upper(),
                                'Gateway Port': port,
                                'Gateway Number': gateway_number,
                                'Phone': originator,
                                'SMS Timestamp': receive_date,
                                'SMS Hour': hour,
                                'Event Name': event,
                                'SMS Votes': votes[:-1],
                                'SMS Invalid': invalid,
                                'SMS Total Voters': total_votes, 
                                'Final Votes': votes[:-1],
                                'Invalid Votes': invalid,
                                'Complete': scto,
                                'Status': status,
                                'Note': note
                            }

                            raw_sms_status = 'Accepted'

                            # Load the JSON file into a dictionary
                            with open('uid_data.json', 'r') as json_file:
                                uid_dict = json.load(json_file)

                            # Forward data to Bubble database
                            _id = uid_dict[uid.upper()]
                            requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)

                # # Return the message to the sender via SMS Gateway
                # params = {
                #     "user": "taufikadinugraha_api",
                #     "password": "SekarangSeriusSMS@ku99",
                #     "SMSText": message,
                #     "GSM": originator,
                #     "output": "json",
                # }
                # requests.get(url_send_sms, params=params)

            except:
                error_type = 1

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
    
    headers = {
        'Authorization': f'Bearer {API_KEY}', 
        'Content-Type': 'text/plain'
        }

    # Generate target file
    tools.create_target(event, N_TPS)
    
    # Forward file to Bubble database
    excel_file_path = f'target_{event}.xlsx'
    
    def file_generator():
        with open(excel_file_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename={excel_file_path}"

    # Remove file
    # os.remove(excel_file_path)

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
    data = '\n'.join([f'{{"UID": "{uid}", "event": "ngetes", "Korwil": "{korwil}", "Provinsi": "{provinsi}", "Kab/Kota": "{kab_kota}", "Kecamatan": "{kecamatan}", "Kelurahan": "{kelurahan}"}}' for uid, korwil, provinsi, kab_kota, kecamatan, kelurahan in zip(df['UID'], df['Korwil'], df['Provinsi'], df['Kab/Kota'], df['Kecamatan'], df['Kelurahan'])])
    
    # Populate votes table
    requests.post(f'{url_bubble}/Votes/bulk', headers=headers, data=data)

    # Get UIDs and store as json
    filter_params = [{"key": "Event Name", "constraint_type": "text contains", "value": event}]
    filter_json = json.dumps(filter_params)
    params = {"constraints": filter_json}
    res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
    uid_dict = {i['UID']:i['_id'] for i in res.json()['response']['results']}
    with open(f'uid_{event}.json', 'w') as json_file:
        json.dump(uid_dict, json_file)

    # Generate xlsform logic using the target file
    tools.create_xlsform_template(target_file_name, form_title, form_id)
    xlsform_path = f'xlsform_{form_id}.xlsx'

    def file_generator():
        with open(xlsform_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename={xlsform_path}"

    # Remove file after sending the response
    os.remove(target_file_name)
    # os.remove(xlsform_path)

    return response