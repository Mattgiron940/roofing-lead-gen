import json
from ghl_webhook_trigger import send_to_ghl

def process_and_route_lead(lead):
    # Example filtering logic
    if lead.get("zip_code") in ["75001", "75201", "76102"]:
        status, resp = send_to_ghl(lead)
        print(f"GHL Push Status: {status}, Response: {resp}")
    else:
        print("Lead outside target area. Skipping.")
