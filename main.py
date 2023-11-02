import json
import numpy as np
from fastapi import FastAPI, Form

app = FastAPI()


# Endpoint to read the "inbox.txt" file
@app.get("/read")
async def read_inbox():
    try:
        with open("inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"inbox_data": data}
    except FileNotFoundError:
        return {"message": "File not found"}


@app.post("/receive")
async def receive_sms(
    id: int = Form(...),
    originator: str = Form(...),
    msg: str = Form(...),
    receive_date: str = Form(...)
):
    
    # Create a dictionary to store the data
    data = {
        "ID": id,
        "Originator": originator,
        "Message": msg,
        "Receive Date": receive_date
    }

    # Log the received data to a JSON file
    with open("inbox.json", "a") as json_file:
        json.dump(data, json_file)
        json_file.write('\n')  # Add a newline to separate the JSON objects

    try:
        # Split message
        info = msg.split('#')
        event = info[1]

        number_candidates = {
            'pilpres': 2,
        }

        format = 'kk#event#' + '#'.join([f'0{i+1}' for i in range(number_candidates[event])]) + '#invalid'
        template_error_msg = 'cek & kirim kembali dgn format:\n' + format

        # Check Error Type 1 (data completeness)
        if len(info) != number_candidates[event] + 3:
            message = 'Data tidak sesuai, ' + template_error_msg
        else:
            votes = info[2:]
            invalid = info[-1]
            total_votes = np.array(votes).astype(int).sum()
            summary = f'event:{event}\n' + '\n'.join([f'0{i+1}:{votes[i]}' for i in range(number_candidates[event])]) + f'\ninvalid:{invalid}' + f'\ntotal:{total_votes}\n'
            # Check Error Type 2 (maximum votes is 300)
            if total_votes > 300:
                message = summary + 'Jumlah suara melebihi 300, ' + template_error_msg
            else:
                message = summary + 'Data berhasil diterima. Jika ada koreksi, kirim kembali dgn format yg sama:\n' + format

                # Forward data to Bubble API
                output = {
                    'smsID': id,
                    'phone': originator,
                    'smsTime': receive_date,
                    'event': event,
                    'smsVotes': votes[:-1],
                    'smsInvalid': invalid,
                    'smsTotal': total_votes
                }

        # Return the message as the API response
        return {"message": message}
    
    except:
        pass