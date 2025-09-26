import os
import requests
import time
import urllib.parse
import hmac
import hashlib
import base64

"""
This example uses SAS tokens for authentication.
The event is sent with a simple POST request to the /messages endpoint of the Event Hub.
"""

NAMESPACE = os.getenv("NAMESPACE")
EVENT_HUB = os.getenv("EVENT_HUB")
KEY_NAME = os.getenv("KEY_NAME")
KEY_VALUE = os.getenv("KEY_VALUE")

def generate_sas_token(uri, key_name, key_value, expiry=3600):
    ttl = int(time.time()) + expiry
    encoded_uri = urllib.parse.quote_plus(uri)
    sign_key = f"{encoded_uri}\n{ttl}".encode("utf-8")
    signature = base64.b64encode(
        hmac.new(key_value.encode("utf-8"), sign_key, hashlib.sha256).digest()
    ).decode("utf-8")

    return f"SharedAccessSignature sr={encoded_uri}&sig={urllib.parse.quote(signature)}&se={ttl}&skn={key_name}"


uri = f"https://{NAMESPACE}.servicebus.windows.net/{EVENT_HUB}"
sas_token = generate_sas_token(uri, KEY_NAME, KEY_VALUE)
headers = {
    "Authorization": sas_token,
    "Content-Type": "application/json",
}

payload = 'Test message from producer_over_https.py'

response = requests.post(f"{uri}/messages", headers=headers, data=payload)

if response.status_code == 201:
    print("Message sent successfully!")
else:
    print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")