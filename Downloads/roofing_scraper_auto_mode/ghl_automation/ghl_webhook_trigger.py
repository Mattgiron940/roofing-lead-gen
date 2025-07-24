import os, requests, json

def send_to_ghl(lead_data):
    url = os.getenv("GHL_WEBHOOK_URL", "https://hooks.placeholder.com")
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=json.dumps(lead_data), headers=headers)
    return response.status_code, response.text
