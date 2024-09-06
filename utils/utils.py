import os
import json
from fastapi import Form
from config.config import *



async def read_sms_inbox():
    """
    Reads the SMS inbox from a JSON file and returns the data.
    """
    try:
        with open(f"{local_disk}/sms_inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"sms_inbox": data}
    except FileNotFoundError:
        return {"message": "File not found"}



async def read_wa_inbox():
    """
    Reads the WhatsApp inbox from a JSON file and returns the data.
    """
    try:
        with open(f"{local_disk}/wa_inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"wa_inbox": data}
    except FileNotFoundError:
        return {"message": "File not found"}



# Function to delete event-related files
async def delete_event(event: str = Form(...), form_id: str = Form(...)):
    event = event.lower()
    os.system(f'rm -f {local_disk}/*_{event}.*')
    os.system(f'rm -f {local_disk}/*_{form_id}.*')


# Function to create a JSON file with the number of candidates for a given event
async def create_json_ncandidate(event: str = Form(...), N_candidate: int = Form(...)):
    event = event.lower()
    with open(f'{local_disk}/event_{event}.json', 'w') as json_file:
        json.dump({"n_candidate": N_candidate}, json_file)


