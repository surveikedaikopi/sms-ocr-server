import re
import json
import random
import numpy as np
import pandas as pd
from Bio import Align
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime, timedelta
from config.config import headers, local_disk, url_bubble, SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD
import threading
import requests

# Load the shapefile
shapefile_path = 'data/location.shp'
gdf = gpd.read_file(shapefile_path)
gdf.crs = "EPSG:4326"

# Load region data from JSON
with open('data/region.json', 'r') as json_file:
    region_data = json.load(json_file)

# Create a threading lock for synchronization
print_lock = threading.Lock()

# List of provinces
list_provinsi = sorted(region_data.keys())

def rename_region(data):
    """
    Rename regions based on the closest match in the region data.
    """
    reference = list_provinsi
    provinsi = find_closest_string(data[0], reference, 'Provinsi')
    reference = list(region_data[provinsi].keys())
    kabkota = find_closest_string(data[1], reference, 'Kab/Kota')
    reference = list(region_data[provinsi][kabkota].keys())
    kecamatan = find_closest_string(data[2], reference, 'Kecamatan')
    reference = list(region_data[provinsi][kabkota][kecamatan])
    kelurahan = find_closest_string(data[3], reference, 'Kelurahan')
    return provinsi, kabkota, kecamatan, kelurahan

def preprocess_text(text):
    """
    Preprocess text by removing non-alphanumeric characters and converting to lowercase.
    """
    return re.sub(r'\W+', '', text.lower())

def compare_sequences(seq1, seq2):
    """
    Compare two sequences using pairwise alignment and return the best alignment score.
    """
    aligner = Align.PairwiseAligner()
    alignments = aligner.align(seq1, seq2)
    return alignments[0].score

def compare_with_list(string1, string2_list):
    """
    Compare a string with a list of strings and return the scores.
    """
    return [compare_sequences(string1, seq2) for seq2 in string2_list]

def find_closest_string(string1, string_list, region):
    """
    Find the closest matching string in a list based on alignment scores.
    """
    if region == 'Kab/Kota':
        first_string = string1.split(' ')[0].lower()
        if first_string != 'kota' and first_string not in ['kab.', 'kabupaten', 'kab']:
            string1 = 'Kab. ' + string1
    preprocessed_string_list = [preprocess_text(s) for s in string_list]
    preprocessed_target = preprocess_text(string1)
    scores = compare_with_list(preprocessed_target, preprocessed_string_list)
    ss = [len([i for i in list(s2) if i not in list(preprocessed_target)]) for s2 in preprocessed_string_list]
    tt = [np.sum([preprocessed_target.count(t1) for t1 in list(t2)])/len(preprocessed_target) for t2 in preprocessed_string_list]
    scores = np.array(scores) - np.array(ss) - np.array(tt)
    return string_list[np.argmax(scores)]

def get_location(coordinate):
    """
    Get location details based on coordinates.
    """
    point = Point(coordinate)
    selected_row = gdf[gdf.geometry.contains(point)]
    kabkota = selected_row['Kab/Kota'].values[0]
    kabkota = f'Kab. {kabkota}' if kabkota.split(' ')[0] not in ['Kab.', 'Kota'] else kabkota
    return {
        'Provinsi': selected_row['Provinsi'].values[0],
        'Kab/Kota': kabkota,
        'Kecamatan': selected_row['Kecamatan'].values[0],
        'Kelurahan': selected_row['Kelurahan'].values[0]
    }

def generate_code():
    """
    Generate a random 3-character code.
    """
    characters = 'abcdefghjkmnpqrstuvwxyz123456789'
    code = ''.join([random.choice(characters) for i in range(3)])
    return code.upper()

def generate_unique_codes(N):
    """
    Generate N unique 3-character codes.
    """
    codes = set()
    while len(codes) < N:
        codes.add(generate_code())
    return list(codes)

def create_target(event, N):
    """
    Create a target Excel file with unique codes.
    """
    event = event.lower()
    df = pd.DataFrame(columns=['UID', 'Korprov', 'Korwil', 'Provinsi', 'Kab/Kota', 'Kecamatan', 'Kelurahan'])
    df['UID'] = generate_unique_codes(N)
    with pd.ExcelWriter(f'{local_disk}/target_{event}.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='survey')

def create_xlsform_template(target_file, form_title, form_id, event):
    """
    Create an XLSForm template based on the target file.
    """
    event = event.lower()
    target_data = pd.read_excel(target_file)
    list_uid = '|'.join(target_data['UID'].tolist())
    
    survey_df = pd.DataFrame([
        {'type': 'text', 'name': 'UID', 'label': 'Masukkan UID (3 karakter) yang sama dengan UID SMS', 'required': 'yes', 'constraint': f"string-length(.) = 3 and regex(., '^({list_uid})$')", 'constraint message': 'UID tidak terdaftar'},
        {'type': 'select_one list_provinsi', 'name': 'selected_provinsi', 'label': 'Pilih Provinsi', 'required': 'yes'},
        {'type': 'select_one list_kabkota', 'name': 'selected_kabkota', 'label': 'Pilih Kabupaten/Kota', 'required': 'yes', 'choice_filter': 'filter_provinsi=${selected_provinsi}'},
        {'type': 'select_one list_kecamatan', 'name': 'selected_kecamatan', 'label': 'Pilih Kecamatan', 'required': 'yes', 'choice_filter': 'filter_provinsi=${selected_provinsi} and filter_kabkota=${selected_kabkota}'},
        {'type': 'select_one list_kelurahan', 'name': 'selected_kelurahan', 'label': 'Pilih Kelurahan', 'required': 'yes', 'choice_filter': 'filter_provinsi=${selected_provinsi} and filter_kabkota=${selected_kabkota} and filter_kecamatan=${selected_kecamatan}'},
        {'type': 'begin_group', 'name': 'upload', 'label': 'Bagian untuk mengunggah/upload foto formulir C1'},
        {'type': 'image', 'name': 'formulir_c1_a4', 'label': 'Foto Formulir C1-A4', 'required': 'yes'},
        {'type': 'image', 'name': 'formulir_c1_plano', 'label': 'Foto Formulir C1-Plano', 'required': 'yes'},
        {'type': 'end_group', 'name': 'upload'},
        {'type': 'geopoint', 'name': 'koordinat', 'label': 'Koordinat Lokasi (GPS)', 'required': 'yes'},
        {'type': 'image', 'name': 'selfie', 'label': 'Masukkan foto Anda yang sedang berada di TPS (diusahakan di samping tanda nomor TPS)', 'required': 'yes'},
        {'type': 'text', 'name': 'nama', 'label': 'Nama Anda', 'required': 'yes'},
        {'type': 'text', 'name': 'no_hp', 'label': 'No. HP Anda', 'required': 'yes'}
    ])
    
    for n, l in zip(['dapil', 'no_tps', 'alamat', 'rt', 'rw'], ['Daerah Pemilihan (Dapil)', 'No. TPS', 'Alamat', 'RT', 'RW']):
        survey_df = survey_df.append({'type': 'text', 'name': n, 'label': l, 'required': 'yes'}, ignore_index=True)
    
    with pd.ExcelWriter(f'{local_disk}/xlsform_{form_id}.xlsx', engine='openpyxl') as writer:
        survey_df.to_excel(writer, index=False, sheet_name='survey')
    
    nested_target = {}
    for row in target_data.itertuples(index=False):
        provinsi, kab_kota, kecamatan, kelurahan = row[3:7]
        if provinsi:
            nested_target.setdefault(provinsi, {})
        if kab_kota and provinsi in nested_target:
            nested_target[provinsi].setdefault(kab_kota, {})
        if kecamatan and provinsi in nested_target and kab_kota in nested_target[provinsi]:
            nested_target[provinsi][kab_kota].setdefault(kecamatan, [])
        if kelurahan and provinsi in nested_target and kab_kota in nested_target[provinsi] and kecamatan in nested_target[provinsi][kab_kota]:
            nested_target[provinsi][kab_kota][kecamatan].append(kelurahan)
    
    choices_df = pd.DataFrame(columns=['list_name', 'name', 'label', 'filter_provinsi', 'filter_kabkota', 'filter_kecamatan'])
    for p in sorted(nested_target.keys()):
        choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_provinsi', 'name': ['_'.join(p.split(' '))], 'label': [p]}))
        for kk in sorted(nested_target[p].keys()):
            choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_kabkota', 'name': ['_'.join(kk.split(' '))], 'label': [kk], 'filter_provinsi': '_'.join(p.split(' '))}))
            for kec in sorted(nested_target[p][kk].keys()):
                choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_kecamatan', 'name': ['_'.join(kec.split(' '))], 'label': [kec], 'filter_provinsi': '_'.join(p.split(' ')), 'filter_kabkota': '_'.join(kk.split(' '))}))
                for kel in sorted(nested_target[p][kk][kec]):
                    choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_kelurahan', 'name': ['_'.join(kel.split(' '))], 'label': [kel], 'filter_provinsi': '_'.join(p.split(' ')), 'filter_kabkota': '_'.join(kk.split(' ')), 'filter_kecamatan': '_'.join(kec.split(' '))}))
    
    with pd.ExcelWriter(f'{local_disk}/xlsform_{form_id}.xlsx', engine='openpyxl', mode='a') as writer:
        choices_df.to_excel(writer, index=False, sheet_name='choices')
    
    settings_df = pd.DataFrame({'form_title': [form_title], 'form_id': [form_id]})
    with pd.ExcelWriter(f'{local_disk}/xlsform_{form_id}.xlsx', engine='openpyxl', mode='a') as writer:
        settings_df.to_excel(writer, index=False, sheet_name='settings')

def scto_process(data, event, n_candidate, proc_id_a4):
    """
    Process SCTO data and update the Bubble server.
    """
    event = event.lower()
    try:
        uid = data['UID']
        std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p") + timedelta(hours=7)
        filter_params = [{"key": "UID", "constraint_type": "equals", "value": uid}]
        res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params={"constraints": json.dumps(filter_params)})
        data_bubble = res_bubble.json()['response']['results'][0]
        
        validator = data_bubble.get('Validator')
        sms_timestamp = data_bubble.get('SMS Timestamp')
        delta_time_hours = None
        if sms_timestamp:
            delta_time = abs(std_datetime - datetime.strptime(sms_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"))
            delta_time_hours = delta_time.total_seconds() / 3600
        
        coordinate = np.array(data['koordinat'].split(' ')[1::-1]).astype(float)
        loc = get_location(coordinate)
        key = data['KEY'].split('uuid:')[-1]
        link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"
        formulir_c1_a4 = data['formulir_c1_a4']
        formulir_c1_plano = data['formulir_c1_plano']
        selfie = data['selfie']

        if proc_id_a4:
            try:
                attachment_url = data['formulir_c1_a4']
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
        
        sms = data_bubble['SMS']
        status = 'Verified' if sms and np.array_equal(np.array(ai_votes).astype(int), np.array(data_bubble['SMS Votes']).astype(int)) and int(ai_invalid) == int(data_bubble['SMS Invalid']) else 'Not Verified'
        gps_status = 'Verified' if all(data_bubble[k] == loc[k] for k in ['Provinsi', 'Kab/Kota', 'Kecamatan', 'Kelurahan']) else 'Not Verified'
        
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
        
        with open(f'{local_disk}/uid_{event}.json', 'r') as json_file:
            uid_dict = json.load(json_file)
        _id = uid_dict[uid.upper()]
        out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
        print(out)
    except Exception as e:
        with print_lock:
            print(f'Process: scto_process\t Keyword: {e}')