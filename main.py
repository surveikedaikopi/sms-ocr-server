import time
import threading
from fastapi import FastAPI
from collections import defaultdict
from fastapi.middleware.cors import CORSMiddleware

from utils.utils import *
from utils.preprocess import *
from utils.postprocess import *
from config.config import *
from controllers.sms import *
from controllers.scto import *
from controllers.media import *
from controllers.whatsapp import *




# ================================================================================================================
# Initial Setup

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

# GET
app.get("/wa_inbox")(read_wa_inbox)
app.get("/sms_inbox")(read_sms_inbox)
app.get("/api/quickcount_kedaikopi")(quickcount_kedaikopi)

# POST
app.post("/getUID")(get_uid)
app.post("/scto_data")(scto_data)
app.post("/delete_event")(delete_event)
app.post("/generate_xlsform")(generate_xlsform)
app.post("/receive_media_info")(receive_media_info)
app.post("/create_json_ncandidate")(create_json_ncandidate)
app.post("/check_gateway_status_sms")(check_gateway_status_sms)





# Define the number of endpoints
num_sms_endpoints = 4
num_whatsapp_endpoints = 16

# Endpoint to receive SMS message, to validate, and to forward the pre-processed data
for port in range(1, num_sms_endpoints + 1):
    app.post(f"/sms-receive-{port}")(receive_sms)

# Endpoint to receive WhatsApp message, to validate, and to forward the pre-processed data
for port in range(1, num_whatsapp_endpoints + 1):
    app.post(f"/wa-receive-{port}")(receive_whatsapp)





# ================================================================================================================
# Scheduler For Quickcount Aggregation

# Global flag to ensure the scheduler runs only once
scheduler_started = False
scheduler_lock = threading.Lock()

@app.on_event("startup")
def startup_event():
    global scheduler_started
    with scheduler_lock:
        if not scheduler_started:
            scheduler_started = True
            fetch_thread = threading.Thread(target=scheduled_fetch_quickcount, daemon=True)
            fetch_thread.start()


def scheduled_fetch_quickcount():
    while True:
        try:
            fetch_quickcount()
        except Exception as e:
            print(f"Error in fetch_quickcount: {str(e)}")
        time.sleep(interval_aggregate)