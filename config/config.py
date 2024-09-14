import os
from dotenv import load_dotenv

# Load environment variables from the parent directory
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

local_disk = os.environ.get('local_disk')
url_send_sms = os.environ.get('url_send_sms')
url_send_wa = os.environ.get('url_send_wa')
url_bubble = os.environ.get('url_bubble')
url_getUID = os.environ.get('url_getUID')
url_get_event_ids = os.environ.get('url_get_event_ids')
url_votes_agg_pilpres = os.environ.get('url_votes_agg_pilpres')
url_votes_agg_provinsi = os.environ.get('url_votes_agg_provinsi')
url_votes_agg_kabkota = os.environ.get('url_votes_agg_kabkota')
BUBBLE_API_KEY = os.environ.get('BUBBLE_API_KEY')
SCTO_SERVER_NAME = os.environ.get('SCTO_SERVER_NAME')
SCTO_USER_NAME = os.environ.get('SCTO_USER_NAME')
SCTO_PASSWORD = os.environ.get('SCTO_PASSWORD')
NUSA_USER_NAME = os.environ.get('NUSA_USER_NAME')
NUSA_PASSWORD = os.environ.get('NUSA_PASSWORD')
NUSA_API_KEY = os.environ.get('NUSA_API_KEY')
list_WhatsApp_Gateway = {
    1: os.environ.get('WA_GATEWAY_1'),
    2: os.environ.get('WA_GATEWAY_2'),
    3: os.environ.get('WA_GATEWAY_3'),
    4: os.environ.get('WA_GATEWAY_4'),
    5: os.environ.get('WA_GATEWAY_5'),
    6: os.environ.get('WA_GATEWAY_6'),
    7: os.environ.get('WA_GATEWAY_7'),
    8: os.environ.get('WA_GATEWAY_8'),
    9: os.environ.get('WA_GATEWAY_9'),
    10: os.environ.get('WA_GATEWAY_10'),
    11: os.environ.get('WA_GATEWAY_11'),
    12: os.environ.get('WA_GATEWAY_12'),
    13: os.environ.get('WA_GATEWAY_13'),
    14: os.environ.get('WA_GATEWAY_14'),
    15: os.environ.get('WA_GATEWAY_15'),
    16: os.environ.get('WA_GATEWAY_16')
}
headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}
headers_bulk = {'Authorization': f'Bearer {BUBBLE_API_KEY}', 'Content-Type': 'text/plain'}
interval_aggregate = os.environ.get('interval_aggregate')