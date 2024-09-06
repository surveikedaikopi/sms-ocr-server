import os
import re
import json
import time
import random
import requests
import threading
import numpy as np
import pandas as pd
from Bio import Align
import geopandas as gpd
from dotenv import load_dotenv
from shapely.geometry import Point
from pysurveycto import SurveyCTOObject
from datetime import datetime, timedelta



# ================================================================================================================
# Initial Setup

# Load env
load_dotenv()

# Load the shapefile
shapefile_path = 'location.shp'
gdf = gpd.read_file(shapefile_path)
gdf.crs = "EPSG:4326"

# Load region data from JSON
with open('region.json', 'r') as json_file:
    region_data = json.load(json_file)

# Create a threading lock for synchronization
print_lock = threading.Lock()

# Global Variables
url_send_sms = os.environ.get('url_send_sms')
url_bubble = os.environ.get('url_bubble')
url_votes_aggregate_pilpres = os.environ.get('url_votes_aggregate_pilpres')
url_votes_aggregate_pilkada = os.environ.get('url_votes_aggregate_pilkada')
local_disk = os.environ.get('local_disk')
BUBBLE_API_KEY = os.environ.get('BUBBLE_API_KEY')
SCTO_SERVER_NAME = os.environ.get('SCTO_SERVER_NAME')
SCTO_USER_NAME = os.environ.get('SCTO_USER_NAME')
SCTO_PASSWORD = os.environ.get('SCTO_PASSWORD')

# Bubble Headers
headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}


# ================================================================================================================
# Auxiliary Functions

# List of provinces
list_provinsi = list(region_data.keys())
list_provinsi.sort()

# Rename regions
def rename_region(data):
    # provinsi
    reference = list_provinsi
    provinsi = find_closest_string(data[0], reference, 'Provinsi')
    # kabupaten/kota
    reference = list(region_data[provinsi].keys())
    kabkota = find_closest_string(data[1], reference, 'Kab/Kota')       
    # kecamatan
    reference = list(region_data[provinsi][kabkota].keys())
    kecamatan = find_closest_string(data[2], reference, 'Kecamatan') 
    # kelurahan
    reference = list(region_data[provinsi][kabkota][kecamatan])
    kelurahan = find_closest_string(data[3], reference, 'Kelurahan')
    return provinsi, kabkota, kecamatan, kelurahan

def preprocess_text(text):
    # Remove spaces and punctuation, convert to lowercase
    return re.sub(r'\W+', '', text.lower())

def compare_sequences(seq1, seq2):
    aligner = Align.PairwiseAligner()
    alignments = aligner.align(seq1, seq2)
    best_alignment = alignments[0]  # Assuming you want the best alignment
    return best_alignment.score

def compare_with_list(string1, string2_list):
    scores = []
    for seq2 in string2_list:
        score = compare_sequences(string1, seq2)
        scores.append(score)
    return scores

def find_closest_string(string1, string_list, region):
    if region == 'Kab/Kota':
        first_string = string1.split(' ')[0].lower()
        if first_string != 'kota':
            if first_string not in ['kab.', 'kabupaten', 'kab']:
                string1 = 'Kab. ' + string1
    preprocessed_string_list = [preprocess_text(s) for s in string_list]
    preprocessed_target = preprocess_text(string1)
    scores = compare_with_list(preprocessed_target, preprocessed_string_list)
    ss = [len([i for i in list(s2) if i not in list(preprocessed_target)]) for s2 in preprocessed_string_list]
    tt = [np.sum([preprocessed_target.count(t1) for t1 in list(t2)])/len(preprocessed_target) for t2 in preprocessed_string_list]
    scores = np.array(scores) - np.array(ss) - np.array(tt)
    closest_index = np.argmax(scores)
    return string_list[closest_index]

# Get administrative regions from coordinate
def get_location(coordinate):
    # Create a Shapely Point object from the input coordinate
    point = Point(coordinate)
    # Check which polygon contains the point
    selected_row = gdf[gdf.geometry.contains(point)]
    # For Kab/Kota only
    kabkota = selected_row['Kab/Kota'].values[0]
    kabkota = f'Kab. {kabkota}' if kabkota.split(' ')[0] not in ['Kab.', 'Kota'] else kabkota
    # Output
    out = {
        'Provinsi': selected_row['Provinsi'].values[0],
        'Kab/Kota': kabkota,
        'Kecamatan': selected_row['Kecamatan'].values[0],
        'Kelurahan': selected_row['Kelurahan'].values[0]
    }
    return out



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
    event = event.lower()
    df = pd.DataFrame(columns=['UID', 'Korprov', 'Korwil', 'Provinsi', 'Kab/Kota', 'Kecamatan', 'Kelurahan'])
    # Generate unique IDs
    df['UID'] = generate_unique_codes(N)
    # Save excel file
    with pd.ExcelWriter(f'{local_disk}/target_{event}.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='survey')



# ================================================================================================================
# Function to generate SCTO xlsform

def create_xlsform_template(target_file, form_title, form_id, event):

    event = event.lower()

    # Load target data from Excel
    target_data = pd.read_excel(target_file)

    # List UID
    list_uid = '|'.join(target_data['UID'].tolist())
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the survey sheet
    survey_df = pd.DataFrame(columns=['type', 'name', 'label', 'required', 'choice_filter', 'calculation', 'constraint', 'constraint message'])

    # default fields
    survey_df['type'] = ['start', 'end', 'deviceid', 'phonenumber', 'username', 'calculate', 'calculate', 'caseid', 'calculate']
    survey_df['name'] = ['starttime', 'endtime', 'deviceid', 'devicephonenum', 'username', 'device_info', 'duration', 'caseid', 'event']
    survey_df['calculation'] = ['', '', '', '', '', 'device-info()', 'duration()', '', event]
    
    # UID
    survey_df = survey_df.append({'type': 'text',
                                  'name': 'UID',
                                  'label': 'Masukkan UID (3 karakter) yang sama dengan UID SMS',
                                  'required': 'yes',
                                  'constraint': f"string-length(.) = 3 and regex(., '^({list_uid})$')",
                                  'constraint message': 'UID tidak terdaftar'
                                 }, ignore_index=True)    
        
    # Regions 
    survey_df = survey_df.append({'type': 'select_one list_provinsi',
                                'name': 'selected_provinsi',
                                'label': 'Pilih Provinsi',
                                'required': 'yes',
                                }, ignore_index=True)
    survey_df = survey_df.append({'type': 'select_one list_kabkota',
                                'name': 'selected_kabkota',
                                'label': 'Pilih Kabupaten/Kota',
                                'required': 'yes',
                                'choice_filter': 'filter_provinsi=${selected_provinsi}',
                                }, ignore_index=True)
    survey_df = survey_df.append({'type': 'select_one list_kecamatan',
                                'name': 'selected_kecamatan',
                                'label': 'Pilih Kecamatan',
                                'required': 'yes',
                                'choice_filter': 'filter_provinsi=${selected_provinsi} and filter_kabkota=${selected_kabkota}',
                                }, ignore_index=True)
    survey_df = survey_df.append({'type': 'select_one list_kelurahan',
                                'name': 'selected_kelurahan',
                                'label': 'Pilih Kelurahan',
                                'required': 'yes',
                                'choice_filter': 'filter_provinsi=${selected_provinsi} and filter_kabkota=${selected_kabkota} and filter_kecamatan=${selected_kecamatan}',
                                }, ignore_index=True)

    # Address
    for (n, l) in zip(['dapil', 'no_tps', 'alamat', 'rt', 'rw'], ['Daerah Pemilihan (Dapil)', 'No. TPS', 'Alamat', 'RT', 'RW']):
        survey_df = survey_df.append({'type': 'text',
                                      'name': n,
                                      'label': l,
                                      'required': 'yes',
                                     }, ignore_index=True) 

    # Upload images
    survey_df = survey_df.append({'type': 'begin_group',
                                  'name': 'upload',
                                  'label': 'Bagian untuk mengunggah/upload foto formulir C1',
                                 }, ignore_index=True) 
    for (n, l) in zip(['formulir_c1_a4', 'formulir_c1_plano'], ['Foto Formulir C1-A4', 'Foto Formulir C1-Plano']):
        survey_df = survey_df.append({'type': 'image',
                                      'name': n,
                                      'label': l,
                                      'required': 'yes',
                                     }, ignore_index=True)
    survey_df = survey_df.append({'type': 'end_group',
                                  'name': 'upload',
                                 }, ignore_index=True) 
    
    # GPS
    survey_df = survey_df.append({'type': 'geopoint',
                                  'name': 'koordinat',
                                  'label': 'Koordinat Lokasi (GPS)',
                                  'required': 'yes',
                                 }, ignore_index=True)

    # Personal Info
    txt = 'Masukkan foto Anda yang sedang berada di TPS (diusahakan di samping tanda nomor TPS)'
    for (t, n, l) in zip(['image', 'text', 'text'], ['selfie', 'nama', 'no_hp'], [txt, 'Nama Anda', 'No. HP Anda']):
        survey_df = survey_df.append({'type': t,
                                    'name': n,
                                    'label': l,
                                    'required': 'yes',
                                    }, ignore_index=True)

    # Save choices to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_{form_id}.xlsx', engine='openpyxl') as writer:
        survey_df.to_excel(writer, index=False, sheet_name='survey')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Create a nested dictionary
    nested_target = {}
    for row in target_data.itertuples(index=False):
        provinsi, kab_kota, kecamatan, kelurahan = row[3:7]
        # Check for None values and initialize nested dictionaries
        if provinsi is not None:
            nested_target.setdefault(provinsi, {})
        if kab_kota is not None and provinsi in nested_target:
            nested_target[provinsi].setdefault(kab_kota, {})
        if kecamatan is not None and provinsi in nested_target and kab_kota in nested_target[provinsi]:
            nested_target[provinsi][kab_kota].setdefault(kecamatan, [])
        if kelurahan is not None and provinsi in nested_target and kab_kota in nested_target[provinsi] and kecamatan in nested_target[provinsi][kab_kota]:
            nested_target[provinsi][kab_kota][kecamatan].append(kelurahan)

    # Create a DataFrame for choices
    choices_df = pd.DataFrame(columns=['list_name', 'name', 'label', 'filter_provinsi', 'filter_kabkota', 'filter_kecamatan'])

    # Add provinsi choices
    provinsi = list(nested_target.keys())
    provinsi = sorted(provinsi)
    choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_provinsi', 
                                                 'name': ['_'.join(i.split(' ')) for i in provinsi], 
                                                 'label': provinsi,
                                                }))

    # Add kabupaten_kota choices
    for p in provinsi:
        kab_kota = list(nested_target[p].keys())
        kab_kota = sorted(kab_kota)
        choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_kabkota', 
                                                     'name': ['_'.join(i.split(' ')) for i in kab_kota],
                                                     'label': kab_kota,
                                                     'filter_provinsi': '_'.join(p.split(' '))
                                                    }))

        # Add kecamatan choices
        for kk in kab_kota:
            kecamatan = nested_target[p][kk]
            kecamatan = sorted(kecamatan)
            choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_kecamatan', 
                                                         'name': ['_'.join(i.split(' ')) for i in kecamatan],
                                                         'label': kecamatan,
                                                         'filter_provinsi': '_'.join(p.split(' ')),
                                                         'filter_kabkota': '_'.join(kk.split(' '))
                                                        }))

            # Add kelurahan choices
            for kec in kecamatan:
                kelurahan = nested_target[p][kk][kec]
                kelurahan = sorted(kelurahan)
                choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_kelurahan', 
                                                             'name': ['_'.join(i.split(' ')) for i in kelurahan],
                                                             'label': kelurahan,
                                                             'filter_provinsi': '_'.join(p.split(' ')),
                                                             'filter_kabkota': '_'.join(kk.split(' ')),       
                                                             'filter_kecamatan': '_'.join(kec.split(' '))
                                                            }))

    # Save choices to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_{form_id}.xlsx', engine='openpyxl', mode='a') as writer:
        choices_df.to_excel(writer, index=False, sheet_name='choices')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the settings
    settings_df = pd.DataFrame({'form_title': [form_title], 
                                'form_id': [form_id]
                               })
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Save settings to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_{form_id}.xlsx', engine='openpyxl', mode='a') as writer:
        settings_df.to_excel(writer, index=False, sheet_name='settings')
            


# ================================================================================================================
# Functions to process SCTO data

def scto_process(data, event, n_candidate, proc_id_a4):

    event = event.lower()

    try:

        # UID
        uid = data['UID']

        # SCTO Timestamp
        std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p")
        std_datetime = std_datetime + timedelta(hours=7)

        # Retrieve data with this UID from Bubble database
        filter_params = [{"key": "UID", "constraint_type": "equals", "value": uid}]
        filter_json = json.dumps(filter_params)
        params = {"constraints": filter_json}
        res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
        data_bubble = res_bubble.json()
        data_bubble = data_bubble['response']['results'][0]

        # Get existing validator
        if 'Validator' in data_bubble:
            validator = data_bubble['Validator']
        else:
            validator = None

        # Delta Time
        if 'SMS Timestamp' in data_bubble:
            sms_timestamp = datetime.strptime(data_bubble['SMS Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
            delta_time = abs(std_datetime - sms_timestamp)
            delta_time_hours = delta_time.total_seconds() / 3600
        else:
            delta_time_hours = None

        # GPS location
        coordinate = np.array(data['koordinat'].split(' ')[1::-1]).astype(float)
        loc = get_location(coordinate)
        
        # Survey Link
        key = data['KEY'].split('uuid:')[-1]
        link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"

        # C1-Form attachments
        formulir_c1_a4 = data['formulir_c1_a4']
        formulir_c1_plano = data['formulir_c1_plano']

        # Selfie attachment
        selfie = data['selfie']

        # OCR C1-Form
        if proc_id_a4:
            try:
                attachment_url = data['formulir_c1_a4']
                # Build SCTO connection
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

        # Check if SMS data exists
        sms = data_bubble['SMS']

        # If SMS data exists, check if they are consistent
        if sms:
            if (np.array_equal(np.array(ai_votes).astype(int), np.array(data_bubble['SMS Votes']).astype(int))) and (int(ai_invalid) == int(data_bubble['SMS Invalid'])):
                status = 'Verified'
                validator = 'System'
            else:
                status = 'Not Verified'
        else:
            status = 'SCTO Only'

        # Update GPS status
        if (data_bubble['Provinsi']==loc['Provinsi']) and (data_bubble['Kab/Kota']==loc['Kab/Kota']) and (data_bubble['Kecamatan']==loc['Kecamatan']) and (data_bubble['Kelurahan']==loc['Kelurahan']):
            gps_status = 'Verified'
        else:
            gps_status = 'Not Verified'

        # Payload
        payload = {
            'Active': True,
            'Complete': sms,
            'UID': uid,
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

        # Load the JSON file into a dictionary
        with open(f'{local_disk}/uid_{event}.json', 'r') as json_file:
            uid_dict = json.load(json_file)

        # Forward data to Bubble Votes database
        _id = uid_dict[uid.upper()]
        out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
        print(out)

    except Exception as e:
        with print_lock:
            print(f'Process: scto_process\t Keyword: {e}')




# ================================================================================================================
# Functions to fetch and save quick count results (PILKADA)

def fetch_pilkada_quickcount():

    headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}
    res = requests.get(url_votes_aggregate_pilkada, headers=headers, params=params)
    out = res.json()['response']
    pilkada = out['pilkada']
    vote1 = out['vote 1']
    vote2 = out['vote 2']
    vote3 = out['vote 3']
    vote4 = out['vote 4']
    vote5 = out['vote 5']
    vote6 = out['vote 6']

    df = pd.DataFrame({'Pilkada': pilkada, 'vote1': vote1, 'vote2': vote2, 'vote3': vote3, 'vote4': vote4, 'vote5': vote5, 'vote6': vote6})
    df['valid'] = df.apply(lambda x : x.vote1 + x.vote2 + x.vote3 + x.vote4 + x.vote5 + x.vote6, axis=1)

    data_entry = round(out['data entry'] * 100, 2)
    total = df[['vote1', 'vote2', 'vote3', 'vote4', 'vote5', 'vote6']].sum()

    if total.sum() > 0:

        total = (total / total.sum() * 100).round(2).values

        output = {
            'timestamp': time.time(),
            'data_entry': data_entry,
            'total': list(total),
        }

        with open(f'{local_disk}/results_pilkada_quickcount.json', 'w') as json_file:
            json.dump(output, json_file, indent=2)




# ================================================================================================================
# Functions to fetch and save quick count results (PILPRES)

# def fetch_pilpres_quickcount():

#     try:

#         headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}
#         params = {'Event ID': 'pilpres'}
#         res = requests.get(url_votes_aggregate_pilpres, headers=headers, params=params)
#         out = res.json()['response']
#         provinsi = out['provinsi']
#         vote1 = out['vote 1']
#         vote2 = out['vote 2']
#         vote3 = out['vote 3']

#         df = pd.DataFrame({'Provinsi': provinsi, 'vote1': vote1, 'vote2': vote2, 'vote3': vote3})
#         df['valid'] = df.apply(lambda x : x.vote1 + x.vote2 + x.vote3, axis=1)

#         data_entry = round(out['data entry'] * 100, 2)
#         total = df[['vote1', 'vote2', 'vote3']].sum()

#         if total.sum() > 0:

#             total = (total / total.sum() * 100).round(2).values

#             output = {
#                 'timestamp': time.time(),
#                 'data_entry': data_entry,
#                 'total': list(total),
#             }
#             for prov in list_provinsi:
#                 tmp = df[df['Provinsi']==prov]
#                 total_prov = tmp['valid'].sum()
#                 if total_prov == 0:
#                     output.update({prov: [0, 0, 0]})
#                 else:
#                     output.update({prov: [round(tmp['vote1'].sum() / total_prov * 100, 2), round(tmp['vote2'].sum() / total_prov * 100, 2), round(tmp['vote3'].sum() / total_prov * 100, 2)]})

#             with open(f'{local_disk}/results_pilpres_quickcount.json', 'w') as json_file:
#                 json.dump(output, json_file, indent=2)

#             # Update Bubble datamart
#             res = requests.get(f'{url_bubble}/Pilpres2024', headers=headers)
#             # if datamart is empty
#             divs = [1 if i == 0 else i for i in df['valid']]
#             if res.json()['response']['count'] == 0:
#                 data = '\n'.join([
#                     f'{{"provinsi": "{provinsi}", '
#                     f'"sum": {sum_}, '
#                     f'"vote1": {vote1}, '
#                     f'"vote2": {vote2}, '
#                     f'"vote3": {vote3}}}'
#                     for provinsi, sum_, vote1, vote2, vote3 in zip(
#                         df['Provinsi'],
#                         df['valid'],
#                         df['vote1']/divs*100,
#                         df['vote2']/divs*100,
#                         df['vote3']/divs*100
#                     )
#                 ])
#                 # Populate datamart in bulk
#                 headers = {
#                     'Authorization': f'Bearer {BUBBLE_API_KEY}', 
#                     'Content-Type': 'text/plain'
#                     }
#                 out = requests.post(f'{url_bubble}/Pilpres2024/bulk', headers=headers, data=data)
#             # if datamart is NOT empty
#             else:
#                 for k, id_ in enumerate([i['_id'] for i in res.json()['response']['results']]):
#                     payload = {
#                         'provinsi': df.loc[k, 'Provinsi'],
#                         'sum': df.loc[k, 'valid'],
#                         'vote1': df.loc[k, 'vote1']/divs[k]*100,
#                         'vote2': df.loc[k, 'vote2']/divs[k]*100,
#                         'vote3': df.loc[k, 'vote3']/divs[k]*100
#                     }
#                     requests.patch(f'{url_bubble}/Pilpres2024/{id_}', headers=headers, data=payload)
#                     time.sleep(2)
    
#         else:
#             output = {
#                 'timestamp': time.time(),
#                 'data_entry': 0,
#                 'total': [0,0,0],
#             }
#             for prov in list_provinsi:
#                 output.update({prov: [0, 0, 0]})

#             with open(f'{local_disk}/results_pilpres_quickcount.json', 'w') as json_file:
#                 json.dump(output, json_file, indent=2)     

#     except Exception as e:
#         output = {
#             'timestamp': time.time(),
#             'data_entry': 0,
#             'total': [0,0,0],
#         }
#         for prov in list_provinsi:
#             output.update({prov: [0, 0, 0]})

#         with open(f'{local_disk}/results_pilpres_quickcount.json', 'w') as json_file:
#             json.dump(output, json_file, indent=2)

#         data = '\n'.join([
#             f'{{"provinsi": "{provinsi}", '
#             f'"sum": 0, '
#             f'"vote1": 0, '
#             f'"vote2": 0, '
#             f'"vote3": 0}}'
#             for provinsi in list_provinsi
#         ])
#         # Populate datamart in bulk
#         headers = {
#             'Authorization': f'Bearer {BUBBLE_API_KEY}', 
#             'Content-Type': 'text/plain'
#             }
#         out = requests.post(f'{url_bubble}/Pilpres2024/bulk', headers=headers, data=data)

#         with print_lock:
#             print(f'Process: fetch_quickcount\t Keyword: {e}')