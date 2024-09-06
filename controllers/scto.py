import os
import json
import time
import requests
import numpy as np
import pandas as pd
from fastapi import Form, UploadFile
from datetime import datetime, timedelta
from pysurveycto import SurveyCTOObject
from fastapi.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor
from config.config import url_getUID, local_disk, url_bubble, headers, headers_populate_votes, SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD
from utils.utils import *

# Function to create a JSON file with the number of candidates for a given event
async def create_json_ncandidate(event: str = Form(...), N_candidate: int = Form(...)):
    event = event.lower()
    with open(f'{local_disk}/event_{event}.json', 'w') as json_file:
        json.dump({"n_candidate": N_candidate}, json_file)

# Function to generate a UID and return an Excel file with the target data
async def get_uid(event: str = Form(...), N_TPS: int = Form(...)):
    event = event.lower()
    tools.create_target(event, N_TPS)
    
    excel_file_path = f'{local_disk}/target_{event}.xlsx'
    
    def file_generator():
        with open(excel_file_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename=target_{event}.xlsx"

    return response

# Function to generate an XLSForm based on the provided target file and form details
async def generate_xlsform(
    form_title: str = Form(...),
    form_id: str = Form(...),
    target_file_name: str = Form(...),
    target_file: UploadFile = Form(...),
):
    event = target_file_name.split('_')[-1].split('.')[0].lower()

    # Save the target file to a temporary location
    with open(f'{local_disk}/{target_file_name}', 'wb') as target_file_content:
        target_file_content.write(target_file.file.read())

    # Get UIDs from the target file
    df = pd.read_excel(f'{local_disk}/{target_file_name}')

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
    df.to_excel(f'{local_disk}/{target_file_name}', index=False)

    # Break into batches
    n_batches = int(np.ceil(len(df) / 100))

    for batch in range(n_batches):
        start = batch * 100
        end = min((batch + 1) * 100, len(df)) - 1 
        tdf = df.loc[start:end, :]

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
                tdf['UID'],
                tdf['Korprov'],
                tdf['Korwil'],
                tdf['Provinsi'],
                tdf['Kab/Kota'],
                tdf['Kecamatan'],
                tdf['Kelurahan'],
                tdf['Provinsi Ori'],
                tdf['Kab/Kota Ori'],
                tdf['Kecamatan Ori'],
                tdf['Kelurahan Ori']
            )
        ])

        # Populate votes table in bulk
        requests.post(f'{url_bubble}/Votes/bulk', headers=headers_populate_votes, data=data)

        time.sleep(3)

    # Get UIDs and store as json
    uid_dict = {}
    for uid_start in range(1, len(df), 50):
        params = {'Event ID': event, 'start': uid_start, 'end': uid_start+50}
        res = requests.get(url_getUID, headers=headers, params=params)
        out = res.json()['response']
        uid_dict.update(zip(out['UID'], out['id_']))

    with open(f'{local_disk}/uid_{event}.json', 'w') as json_file:
        json.dump(uid_dict, json_file)

    # Generate xlsform logic using the target file
    tools.create_xlsform_template(f'{local_disk}/{target_file_name}', form_title, form_id, event)
    xlsform_path = f'{local_disk}/xlsform_{form_id}.xlsx'

    def file_generator():
        with open(xlsform_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename=xlsform_{form_id}.xlsx"

    return response

# Function to delete event-related files
async def delete_event(event: str = Form(...), form_id: str = Form(...)):
    event = event.lower()
    os.system(f'rm -f {local_disk}/*_{event}.*')
    os.system(f'rm -f {local_disk}/*_{form_id}.*')

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
                    executor.submit(tools.scto_process, data, event, n_candidate, proc_id_a4)
    
    except Exception as e:
        print(f'Process: scto_data endpoint\t Keyword: {e}\n')