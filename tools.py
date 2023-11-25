import json
import random
import pandas as pd

# ================================================================================================================
# Functions to generate UID

def generate_code():
    characters = 'abcdefghjkmnpqrstuvwxyz123456789'
    code = ''.join([random.choice(characters) for i in range(3)])
    return code.upper()

def generate_unique_codes(N):
    codes = []
    while len(codes) < N:
        code = generate_code()
        if code not in codes:
            codes.append(code)
    return codes

def create_target(event, N):
    df = pd.DataFrame(columns=['UID', 'Korwil', 'Provinsi', 'Kab/Kota', 'Kecamatan'])
    # Generate unique IDs
    df['UID'] = generate_unique_codes(N)
    # Save UIDs in json file
    with open(f'uid_{event}.json', 'w') as json_file:
        json.dump(df['UID'].tolist(), json_file)
    # Save excel file
    with pd.ExcelWriter(f'target_{event}.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='survey')