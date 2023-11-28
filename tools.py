import json
import random
import pandas as pd

# ================================================================================================================
# Load files

# Load region data from JSON
with open('region.json', 'r') as json_file:
    region_data = json.load(json_file)



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
    # Save UIDs in json file
    with open(f'uid_{event}.json', 'w') as json_file:
        json.dump(df['UID'].tolist(), json_file)
    # Save excel file
    with pd.ExcelWriter(f'target_{event}.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='survey')



# ================================================================================================================
# Function to generate SCTO xlsform

def create_xlsform_template(target_file, form_title, form_id):

    # Load target data from Excel
    target_data = pd.read_excel(target_file)

    # List UID
    list_uid = '|'.join(target_data['UID'].tolist())
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the survey sheet
    survey_df = pd.DataFrame(columns=['type', 'name', 'label', 'required', 'choice_filter', 'calculation', 'constraint', 'constraint message'])

    # default fields
    survey_df['type'] = ['start', 'end', 'deviceid', 'phonenumber', 'username', 'calculate', 'calculate', 'caseid']
    survey_df['name'] = ['starttime', 'endtime', 'deviceid', 'devicephonenum', 'username', 'device_info', 'duration', 'caseid']
    survey_df['calculation'] = ['', '', '', '', '', 'device-info()', 'duration()', '']
    
    # Add uid question
    survey_df = survey_df.append({'type': 'text',
                                  'name': 'UID',
                                  'label': 'Masukkan UID (3 digit) yang sama dengan UID SMS',
                                  'required': 'yes',
                                  'constraint': f"string-length(.) = 3 and regex(., '^({list_uid})$')",
                                  'constraint message': 'UID tidak terdaftar'
                                 }, ignore_index=True)    
        
    # Add provinsi question
    survey_df = survey_df.append({'type': 'select_one list_provinsi',
                                  'name': 'selected_provinsi',
                                  'label': 'Pilih Provinsi',
                                  'required': 'yes'
                                 }, ignore_index=True)

    # Add kabupaten_kota question
    survey_df = survey_df.append({'type': 'select_one list_kabkota',
                                  'name': 'selected_kabupaten_kota',
                                  'label': 'Pilih Kabupaten/Kota',
                                  'required': 'yes',
                                  'choice_filter': 'provinsi=${selected_provinsi}'
                                 }, ignore_index=True)

    # Add kecamatan question
    survey_df = survey_df.append({'type': 'select_one list_kecamatan',
                                  'name': 'selected_kecamatan',
                                  'label': 'Pilih Kecamatan',
                                  'required': 'yes',
                                  'choice_filter': 'kabupaten_kota=${selected_kabupaten_kota}'
                                 }, ignore_index=True)
    
    # Add kelurahan question
    survey_df = survey_df.append({'type': 'select_one list_kelurahan',
                                  'name': 'selected_kelurahan',
                                  'label': 'Pilih Kelurahan',
                                  'required': 'yes',
                                  'choice_filter': 'kecamatan=${selected_kecamatan}'
                                 }, ignore_index=True)

    # Add image & geopoint question
    for (t,n,l,r) in zip(['text', 'image', 'image', 'geopoint'], ['no_tps', 'foto_formulir_c1', 'foto_jumlah_suara', 'lokasi'], ['No. TPS', 'Foto Formulir C1', 'Foto Jumlah Suara di Formulir C1', 'Lokasi'], ['yes', 'yes', 'yes', 'yes']):
        survey_df = survey_df.append({'type': t,
                                      'name': n,
                                      'label': l,
                                      'required': r,
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
            # kecamatan = nested_target[p][kk]
            kecamatan = region_data[p][kk]
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
            