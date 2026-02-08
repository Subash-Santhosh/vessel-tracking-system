import requests
import os
import time
import json
import warnings
from kafka import KafkaProducer

warnings.filterwarnings("ignore")

interval_time = 300

MAX_TOKEN_RETRIES = 3

navtor_username = os.getenv("NAVTOR_USERNAME")
navtor_password = os.getenv("NAVTOR_PASSWORD")
navtor_client_id = os.getenv("NAVTOR_CLIENT_ID")
navtor_client_secret = os.getenv("NAVTOR_CLIENT_SECRET")
navtor_token_url = os.getenv("NAVTOR_TOKEN_URL")

navtor_status_url = os.getenv("NAVTOR_STATUS_URL")

payload = {
        "username": navtor_username,
        "client_secret": navtor_client_secret,
        "client_id": navtor_client_id,
        "password": navtor_password,
        "grant_type": "password"
    }

## Kafka Producer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer = lambda k: k.encode("utf-8"),
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
    )

## API Call
def get_token(url, payload):
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.request("POST",url, headers = headers, data=payload)
    if response.status_code == 200:
        print("Received Navtor Token.")
        response = response.json()["access_token"]
        return response
    else:
        raise AuthError(f"Token request failed: {response.status_code}")
        print("Error Receiving the Navtor Token.")

def get_navtor_data(url,token):
    token = f"Bearer {token}"
    headers = {"Authorization": token, "Content-Type": "application/json", "accept": "application/json"}
    response = requests.request("GET", url, headers= headers)

    if response.status_code == 200:
        print("Received Navtor Data.")
        return response.json()
    else:
        if response.status_code == 401:
            raise AuthError("Token expired or invalid")
        elif response.status_code == 429:
            raise RateLimitError("API rate limit exceeded")
        else:
            raise Exception("Error Receiving the Navtor Data.")

def get_token_with_retry(navtor_token_url,payload):
    for attempt in range(1, MAX_TOKEN_RETRIES + 1):
        try:
            token = get_token(navtor_token_url,payload)
            return token
        except AuthError as e:
            print(f"Auth attempt {attempt} failed: {e}")
            time.sleep(5)

    raise SystemExit("Unable to obtain token after retries")


token = get_token_with_retry(navtor_token_url,payload)

while True:
    try:
        navtor_data = get_navtor_data(navtor_status_url, token)

        if not navtor_data:
            print("No data Fetched")
        else:
            for vessel in navtor_data:
                imo = vessel["imo"]
                if not imo:
                    continue
            
                producer.send("vessel-tracking-system", key=str(imo), value=vessel )

            producer.flush()

    except AuthError:
        print("Auth error. Refreshing token...")
        token = get_token_with_retry(navtor_token_url,payload)

    except RateLimitError:
        print("429 received. Backing off...")
        time.sleep(600)      # wait 10 minutes
        
    except Exception as e:
        print(f"⚠️ Error occurred: {e}")
    
    time.sleep(interval_time)
        