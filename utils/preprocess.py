import re
import time
import json
import random
import requests
import threading
import numpy as np
import pandas as pd
from Bio import Align
import geopandas as gpd
from fastapi import Form
from shapely.geometry import Point
from fastapi import Form, UploadFile
from fastapi.responses import StreamingResponse

from config.config import *



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



# Function to generate a UID and return an Excel file with the target data
async def get_uid(event: str = Form(...), N_TPS: int = Form(...)):
    event = event.lower()
    create_target(event, N_TPS)
    
    excel_file_path = f'{local_disk}/target_{event}.xlsx'
    
    def file_generator():
        with open(excel_file_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename=target_{event}.xlsx"

    return response



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
            if provinsi not in nested_target:
                nested_target[provinsi] = {}
        if kab_kota and provinsi in nested_target:
            if kab_kota not in nested_target[provinsi]:
                nested_target[provinsi][kab_kota] = {}
        if kecamatan and provinsi in nested_target and kab_kota in nested_target[provinsi]:
            if kecamatan not in nested_target[provinsi][kab_kota]:
                nested_target[provinsi][kab_kota][kecamatan] = []
        if kelurahan and provinsi in nested_target and kab_kota in nested_target[provinsi] and kecamatan in nested_target[provinsi][kab_kota]:
            if kelurahan not in nested_target[provinsi][kab_kota][kecamatan]:
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
        output_regions = rename_region(input_regions)
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
        requests.post(f'{url_bubble}/Votes/bulk', headers=headers_bulk, data=data)

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
    create_xlsform_template(f'{local_disk}/{target_file_name}', form_title, form_id, event)
    xlsform_path = f'{local_disk}/xlsform_{form_id}.xlsx'

    def file_generator():
        with open(xlsform_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename=xlsform_{form_id}.xlsx"

    return response