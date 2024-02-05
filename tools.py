import os
import io
import re
import json
import random
import requests
import threading
import numpy as np
import pandas as pd
from Bio import Align
from PIL import Image
import geopandas as gpd
from dotenv import load_dotenv
from shapely.geometry import Point
from google.cloud import documentai
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
    provinsi = find_closest_string(data[0], reference)
    # kabupaten/kota
    reference = list(region_data[provinsi].keys())
    kabkota = find_closest_string(data[1], reference)       
    # kecamatan
    reference = list(region_data[provinsi][kabkota].keys())
    kecamatan = find_closest_string(data[2], reference) 
    # kelurahan
    reference = list(region_data[provinsi][kabkota][kecamatan])
    kelurahan = find_closest_string(data[3], reference)
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

def find_closest_string(string1, string_list):
    preprocessed_string_list = [preprocess_text(s) for s in string_list]
    preprocessed_target = preprocess_text(string1)
    scores = compare_with_list(preprocessed_target, preprocessed_string_list)
    ss = [len([i for i in list(s2) if i not in list(preprocessed_target)]) for s2 in preprocessed_string_list]
    scores = np.array(scores) - np.array(ss)
    closest_index = np.argmax(scores)
    return string_list[closest_index]

# Get administrative regions from coordinate
def get_location(coordinate):
    # Create a Shapely Point object from the input coordinate
    point = Point(coordinate)
    # Check which polygon contains the point
    selected_row = gdf[gdf.geometry.contains(point)]
    # Output
    out = {
        'Provinsi': selected_row['Provinsi'].values[0],
        'Kab/Kota': selected_row['Kab/Kota'].values[0],
        'Kecamatan': selected_row['Kecamatan'].values[0],
        'Kelurahan': selected_row['Kelurahan'].values[0]
    }
    return out


# Document inference
def read_form(scto, attachment_url, n_candidate, processor_id, deviceid):
    project_id = "quick-count-410523"
    location = "us"

    # Initialize the DocumentProcessorServiceClient
    client = documentai.DocumentProcessorServiceClient.from_service_account_file('document-ai.json')
    
    # Construct the processor path
    name = f'projects/{project_id}/locations/{location}/processors/{processor_id}'
        
    # Convert the attachment URL content to a byte array
    file_content = scto.get_attachment(attachment_url)
    
    if deviceid != '(web)':
        pass
        # # Rotate the image 90 degrees clockwise
        # image = Image.open(io.BytesIO(file_content))
        # rotated_image = image.rotate(-90, expand=True)
        # byte_array = io.BytesIO()
        # rotated_image.save(byte_array, format='JPEG')
        # file_content = byte_array.getvalue()

    # Load binary data
    raw_document = documentai.RawDocument(content=file_content, mime_type='image/jpeg')

    # Configure the process request
    request = documentai.ProcessRequest(
        name=name,
        raw_document=raw_document,
    )

    # Process Document
    out = client.process_document(request)
    entities = out.document.entities
    output = {}
    if len(entities) > 0:
        # Valid Votes
        try:
            for entity in entities[0].properties:
                output.update({entity.type_: entity.normalized_value.text})
        except:
            pass
        try:
            # Invalid votes
            entity = entities[1]
            output.update({entity.type_: entity.normalized_value.text})
        except:
            pass
    else:
        ai_votes = [0] * n_candidate
        ai_invalid = 0

    # Post-processing
    ai_votes = [0] * n_candidate
    for var_ in range(n_candidate):
        try:
            ai_votes[var_] = remove_non_numbers_and_convert_to_int(output[f'suara{var_+1}'])
        except:
            ai_votes[var_] = 0
    try:
        ai_invalid = remove_non_numbers_and_convert_to_int(output['suara_rusak'])
    except:
        ai_invalid = 0
            
    return ai_votes, ai_invalid


def remove_non_numbers_and_convert_to_int(input_string):
    # Use a list comprehension to create a string containing only digits
    digits_only = ''.join(char for char in input_string if char.isdigit())
    # Convert the string of digits to an integer
    result_integer = int(digits_only)
    return result_integer



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
        provinsi, kab_kota, kecamatan = row[3:6]
        # Check for None values and initialize nested dictionaries
        if provinsi is not None:
            nested_target.setdefault(provinsi, {})
        if kab_kota is not None and provinsi in nested_target:
            nested_target[provinsi].setdefault(kab_kota, [])
        if kecamatan is not None and provinsi in nested_target and kab_kota in nested_target[provinsi]:
            nested_target[provinsi][kab_kota].append(kecamatan)

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
                kelurahan = region_data[p][kk][kec]
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
        filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid}]
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
# Functions to fetch and save quick count results

def fetch_quickcount():
    events = []  # To store events

    output = {
        'data_entry': 0,
        'total': 0
        }
    output.update({prov:0 for prov in list_provinsi}) 

    # Send a GET request to get events
    response = requests.get(url_bubble + '/Events')

    # Check the response status code
    if response.status_code == 200:
        tmp = response.json()['response']
        events.extend(tmp['results'])

    if len(events) > 0:

        # Send a GET request to get votes
        response = requests.get(url_bubble + '/Votes')

        # Check the response status code
        votes = []
        if response.status_code == 200:
            tmp = response.json()['response']
            votes.extend(tmp['results'])

            df = pd.DataFrame(votes)

            # Filter pilpres only
            df = df[df['Event ID']=='pilpres']
            n_candidate = 3

            if len(df) > 0:

                total_votes = df['Final Votes'].apply(lambda x: np.nansum(x)).sum()
                if total_votes > 0:
                    output = {
                        'data_entry': round(df['SMS'].sum() / len(df) * 100, 2),
                        'total': [round(df[f'Vote{i}'].sum() / total_votes * 100, 2) for i in range(1, n_candidate + 1)]
                    }
                    for prov in list_provinsi:
                        data_prov = df[df['Provinsi']==prov]
                        total_prov = data_prov['Final Votes'].apply(lambda x: np.nansum(x)).sum()
                        if total_prov > 0:
                            output.update({prov: [round(data_prov[f'Vote{i}'].sum() / total_prov * 100, 2) for i in range(1, n_candidate + 1)]})
                        else:
                            output.update({prov: [0, 0, 0]})

    with open(f'{local_disk}/results_quickcount.json', 'w') as json_file:
        json.dump(output, json_file, indent=2)
