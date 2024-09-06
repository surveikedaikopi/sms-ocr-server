from dotenv import load_dotenv
from collections import defaultdict
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI

from controllers.sms import receive_sms, check_gateway_status_sms
from controllers.whatsapp import receive_whatsapp
from controllers.scto import scto_data, create_json_ncandidate, get_uid, generate_xlsform, delete_event
from controllers.bubble import receive_ip_whitelist, pilpres_quickcount_kedaikopi, pilkada_quickcount_kedaikopi, read_sms_inbox, read_wa_inbox, region_aggregate

# ================================================================================================================
# Initial Setup

# Load env
load_dotenv('.env')

# Define app
app = FastAPI(docs_url="/docs", redoc_url=None)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dictionary to store request timestamps for each client IP
request_timestamps = defaultdict(float)

# Time window in seconds
TIME_WINDOW = 60  # 1 minute

# ================================================================================================================
# Endpoints

app.post("/receive_ip_whitelist")(receive_ip_whitelist)
app.get("/api/pilpres_quickcount_kedaikopi")(pilpres_quickcount_kedaikopi)
app.get("/api/pilkada_quickcount_kedaikopi")(pilkada_quickcount_kedaikopi)
app.get("/sms_inbox")(read_sms_inbox)
app.get("/wa_inbox")(read_wa_inbox)
app.post("/check_gateway_status_sms")(check_gateway_status_sms)
app.post("/create_json_ncandidate")(create_json_ncandidate)
app.post("/getUID")(get_uid)
app.post("/generate_xlsform")(generate_xlsform)
app.post("/delete_event")(delete_event)
app.post("/scto_data")(scto_data)
app.post("/group_normalize")(region_aggregate)

# Define the number of endpoints
num_sms_endpoints = 16
num_whatsapp_endpoints = 16

# Endpoint to receive SMS message, to validate, and to forward the pre-processed data
for port in range(1, num_sms_endpoints + 1):
    app.post(f"/sms-receive-{port}")(receive_sms)

# Endpoint to receive WhatsApp message, to validate, and to forward the pre-processed data
for port in range(1, num_whatsapp_endpoints + 1):
    app.post(f"/wa-receive-{port}")(receive_whatsapp)