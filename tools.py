import os
import json
import random
import pandas as pd
import geopandas as gpd
from pytz import timezone
from dotenv import load_dotenv
from shapely.geometry import Point
from google.cloud import documentai


# ================================================================================================================
# Initial Setup

# Load env
load_dotenv()

# # Load the shapefile
# shapefile_path = 'location.shp'
# gdf = gpd.read_file(shapefile_path)
# gdf.crs = "EPSG:4326"

# # Load region data from JSON
# with open('region.json', 'r') as json_file:
#     region_data = json.load(json_file)



# ================================================================================================================
# Auxiliary Functions

# Function to convert datetime to server time zone
def convert_to_server_timezone(dt):
    # Define the time zone of the server
    server_timezone = timezone('UTC')
    return dt.astimezone(server_timezone)


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
def read_form(scto, attachment_url, n_candidate, processor_id):
    project_id = "quickcount-404922"
    location = "us"
    # processor_id = "66c5b23bee13a9d6"

    # Initialize the DocumentProcessorServiceClient
    client = documentai.DocumentProcessorServiceClient()
    
    # Construct the processor path
    name = f'projects/{project_id}/locations/{location}/processors/{processor_id}'
        
    # Convert the attachment URL content to a byte array
    file_content = scto.get_attachment(attachment_url)
    
    # Load binary data
    raw_document = documentai.RawDocument(content=file_content, mime_type='image/jpeg')

    # Configure the process request
    request = documentai.ProcessRequest(
        name=name,
        raw_document=raw_document,
    )

    # Process Document
    out = client.process_document(request)    
    output = {}
    for entity in out.document.entities:
        output.update({entity.type_:entity.mention_text})
    
    # Post-processing
    ai_votes = [0] * n_candidate
    for var_ in range(n_candidate):
        try:
            ai_votes[var_] = remove_non_numbers_and_convert_to_int(output[f'suara{var_+1}'])
        except:
            ai_votes[var_] = 0
    try:
        ai_invalid = remove_non_numbers_and_convert_to_int(output['rusak'])
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
    df = pd.DataFrame(columns=['UID', 'Korwil', 'Provinsi', 'Kab/Kota', 'Kecamatan', 'Kelurahan'])
    # Generate unique IDs
    df['UID'] = generate_unique_codes(N)
    # Save excel file
    with pd.ExcelWriter(f'target_{event}.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='survey')



# ================================================================================================================
# Function to generate SCTO xlsform

def create_xlsform_template(target_file, form_title, form_id, event):

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
        
    # Group 1: Regions & Address
    survey_df = survey_df.append({'type': 'begin_group',
                                  'name': 'regions',
                                  'label': 'Wilayah',
                                 }, ignore_index=True)    
    regions = ['provinsi', 'kabkota', 'kecamatan', 'kelurahan']
    labels = ['Provinsi', 'Kabupaten/Kota', 'Kecamatan', 'Kelurahan']
    for (r, l) in zip([regions, labels]):
        survey_df = survey_df.append({'type': f'select_one list_{r}',
                                    'name': f'selected_{r}',
                                    'label': f'Pilih {l}',
                                    'required': 'yes'
                                    }, ignore_index=True)
    for (n, l) in zip(['no_tps', 'alamat', 'rt', 'rw'], ['No. TPS', 'Alamat', 'RT', 'RW']):
        survey_df = survey_df.append({'type': 'text',
                                      'name': n,
                                      'label': l,
                                      'required': 'yes',
                                     }, ignore_index=True)
    survey_df = survey_df.append({'type': 'end_group',
                                  'name': 'regions',
                                 }, ignore_index=True) 

    # Upload images
    survey_df = survey_df.append({'type': 'begin_group',
                                  'name': 'upload',
                                  'label': 'Bagian untuk mengunggah/upload foto formulir C1',
                                 }, ignore_index=True) 
    for (n, l) in zip(['formulir_c1_a4', 'formulir_c1_plano', 'foto_jumlah_suara'], ['Foto Formulir C1-A4', 'Foto Formulir C1-Plano', 'Foto fokus jarak dekat di bagian jumlah suara']):
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
    for (t, n, l) in zip(['image', 'text', 'text'], ['selfie', 'nama', 'no. hp'], [txt, 'Nama Anda', 'No. HP Anda']):
        survey_df = survey_df.append({'type': t,
                                    'name': n,
                                    'label': l,
                                    'required': 'yes',
                                    }, ignore_index=True)

    # Save choices to an Excel file
    with pd.ExcelWriter(f'xlsform_{form_id}.xlsx', engine='openpyxl') as writer:
        survey_df.to_excel(writer, index=False, sheet_name='survey')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Create a nested dictionary
    nested_target = {}
    for row in target_data.itertuples(index=False):
        provinsi, kab_kota, kecamatan = row[2:-1]
        # Check for None values and initialize nested dictionaries
        if provinsi is not None:
            nested_target.setdefault(provinsi, {})
        if kab_kota is not None and provinsi in nested_target:
            nested_target[provinsi].setdefault(kab_kota, [])
        if kecamatan is not None and provinsi in nested_target and kab_kota in nested_target[provinsi]:
            nested_target[provinsi][kab_kota].append(kecamatan)

    # Create a DataFrame for choices
    choices_df = pd.DataFrame(columns=['list_name', 'name', 'label', 'provinsi', 'kabupaten_kota', 'kecamatan'])

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
                                                     'provinsi': '_'.join(p.split(' '))
                                                    }))

        # Add kecamatan choices
        for kk in kab_kota:
            kecamatan = nested_target[p][kk]
            kecamatan = sorted(kecamatan)
            choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_kecamatan', 
                                                         'name': ['_'.join(i.split(' ')) for i in kecamatan],
                                                         'label': kecamatan,
                                                         'provinsi': '_'.join(p.split(' ')),
                                                         'kabupaten_kota': '_'.join(kk.split(' '))
                                                        }))

            # Add kelurahan choices
            for kec in kecamatan:
                kelurahan = region_data[p][kk][kec]
                kelurahan = sorted(kelurahan)
                choices_df = choices_df.append(pd.DataFrame({'list_name': 'list_kelurahan', 
                                                             'name': ['_'.join(i.split(' ')) for i in kelurahan],
                                                             'label': kelurahan,
                                                             'provinsi': '_'.join(p.split(' ')),
                                                             'kabupaten_kota': '_'.join(kk.split(' ')),                                                           
                                                             'kecamatan': '_'.join(kec.split(' '))
                                                            }))

    # Save choices to an Excel file
    with pd.ExcelWriter(f'xlsform_{form_id}.xlsx', engine='openpyxl', mode='a') as writer:
        choices_df.to_excel(writer, index=False, sheet_name='choices')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the settings
    settings_df = pd.DataFrame({'form_title': [form_title], 
                                'form_id': [form_id]
                               })
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Save settings to an Excel file
    with pd.ExcelWriter(f'xlsform_{form_id}.xlsx', engine='openpyxl', mode='a') as writer:
        settings_df.to_excel(writer, index=False, sheet_name='settings')
            