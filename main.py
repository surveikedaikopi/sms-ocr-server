import os
import json
import tools
import requests
import numpy as np
from fastapi import Form, FastAPI, UploadFile
from fastapi.responses import StreamingResponse

app = FastAPI()

url_send_sms = "https://api.nusasms.com/api/v3/sendsms/plain"
url_bubble = "https://quick-count.bubbleapps.io/version-test/api/1.1/obj"
API_KEY = "cecd6c1aa78871f6746b03bb1997508f"

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
@app.post("/receive")
async def receive_sms(
    id: int = Form(...),
    gateway_number: int = Form(...),
    originator: str = Form(...),
    msg: str = Form(...),
    receive_date: str = Form(...)
):
    
    # Create a dictionary to store the data
    data = {
        "ID": id,
        "Gateway": gateway_number,
        "Sender": originator,
        "Message": msg,
        "Receive Date": receive_date
    }

    # Log the received data to a JSON file
    with open("inbox.json", "a") as json_file:
        json.dump(data, json_file)
        json_file.write('\n')  # Add a newline to separate the JSON objects

    # Split message
    info = msg.lower().split('#')
    uid = info[1]
    event = info[2]

    number_candidates = {
        'pilpres': 2,
    }

    # Check Error Type 1 (prefix)
    if info[0] == 'kk':

        format = 'kk#uid#event#' + '#'.join([f'0{i+1}' for i in range(number_candidates[event])]) + '#rusak'
        template_error_msg = 'cek & kirim ulang dgn format:\n' + format

        # Check Error Type 2 (UID)
        if uid not in list_uid:
            message = 'Unique ID (UID) tidak terdaftar, ' + template_error_msg
        else:
            # Check Error Type 3 (data completeness)
            if len(info) != number_candidates[event] + 3:
                message = 'Data tidak sesuai, ' + template_error_msg
            else:
                votes = info[3:]
                invalid = info[-1]
                total_votes = np.array(votes).astype(int).sum()
                summary = f'event:{event}\n' + '\n'.join([f'0{i+1}:{votes[i]}' for i in range(number_candidates[event])]) + f'\nrusak:{invalid}' + f'\ntotal:{total_votes}\n'
                # Check Error Type 4 (maximum votes)
                if total_votes > 300:
                    message = summary + 'Jumlah suara melebihi 300, ' + template_error_msg
                else:
                    message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama:\n' + format

                    # Payload
                    payload = {
                        'SMS': True,
                        'UID': uid,
                        'Gateway': gateway_number,
                        'Phone': originator,
                        'SMS Timestamp': receive_date,
                        'Event Name': event,
                        'SMS Votes': votes[:-1],
                        'SMS Invalid': invalid,
                        'SMS Total Voters': total_votes
                    }
                    headers = {'Authorization': f'Bearer {API_KEY}'}

                    # Check if data with the same "Phone" number exists in database
                    res = requests.get(f'{url_bubble}/votes?filter[Phone]={originator}', headers=headers)
                    data = res.json()                                    

                    # Forward data to Bubble database
                    if len(data['response']['results']) == 0:
                        # Add new data
                        requests.post(f'{url_bubble}/votes', headers=headers, data=payload)
                    else:
                        _id = data['response']['results'][0]['_id']
                        # Modify existing data
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

    # Remove file
    os.remove(excel_file_path)

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

    # Save the target file to a temporary location
    with open(target_file_name, 'wb') as target_file_content:
        target_file_content.write(target_file.file.read())

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