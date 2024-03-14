import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
# Load the environment variables from the .env file
load_dotenv()

# Define the parameters for the POST request
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI")
RESTAURANT_ID = os.environ.get("RESTAURANT_ID")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
EXPIRES_IN = os.environ.get("EXPIRES_IN")
EXPIRES_AT = os.environ.get("EXPIRES_AT")

rheaders = {'Content-Type': 'application/x-www-form-urlencoded'}
base_url = "https://api.dinlr.com/v1"

def convert_to_datetime(date_string):
    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")

def is_token_expired():
    # Check if the access token has expired
    return datetime.now() >= convert_to_datetime(EXPIRES_AT)

try:
    print("Trying...")
    if is_token_expired():
        # Step 4: Request a new access token using the refresh token
        # Define the parameters for the POST request
        params = {
            "refresh_token": REFRESH_TOKEN,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token"
        }

        # Send the POST request to the token endpoint
        response = requests.post(f"{base_url}/{RESTAURANT_ID}/oauth/token", data=params, headers=rheaders)

        # Parse the JSON response and extract the new access token
        print("getting response...")
        data = response.json()
        print(data)

except Exception as e:
    # Handle any exceptions that may occur
    print(f"An error occurred: {str(e)}")
    print(f"Error type: {type(e).__name__}")
    print(f"Error context: {e.args}")
    print(data)

if data:
    ACCESS_TOKEN = data["access_token"]
    REFRESH_TOKEN = data["refresh_token"]
    EXPIRES_IN = str(data["expires_in"])

    # Update the .env file with the new access token and refresh token
    os.environ["ACCESS_TOKEN"] = ACCESS_TOKEN
    os.environ["EXPIRES_IN"] = EXPIRES_IN
    os.environ["REFRESH_TOKEN"] = REFRESH_TOKEN

    # Declare EXPIRES_AT environment variable
    expires_at = datetime.now() + timedelta(seconds=int(data["expires_in"]))
    os.environ["EXPIRES_AT"] = expires_at.strftime("%Y-%m-%dT%H:%M:%S")
    # os.environ["EXPIRES_AT"] = str(datetime.now() + timedelta(seconds=int(EXPIRES_IN)))

    # export to .env file
    with open('.env', 'w') as f:
        for key, value in os.environ.items():
            f.write(f"{key}={value}\n")