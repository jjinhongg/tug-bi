import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load the environment variables from the .env file
load_dotenv()

# Define the parameters for the POST request
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
AUTHORIZATION_CODE = os.environ.get("AUTHORIZATION_CODE")
REDIRECT_URI = os.environ.get("REDIRECT_URI")
RESTAURANT_ID = os.environ.get("RESTAURANT_ID")

params = {
    "grant_type": "authorization_code",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "code": AUTHORIZATION_CODE
}

headers = {'Content-Type': 'application/x-www-form-urlencoded'}

# Send the POST request to the token endpoint to request an access and a refresh token
response = requests.post(f"https://api.dinlr.com/v1/{RESTAURANT_ID}/oauth/token", data=params, headers=headers)

# Parse the JSON response and extract the access and refresh tokens
data = response.json()
print(data)
ACCESS_TOKEN = data["access_token"]
REFRESH_TOKEN = data["refresh_token"]

# Store the access and refresh tokens in the .env file
os.environ["ACCESS_TOKEN"] = ACCESS_TOKEN
os.environ["AUTHORIZATION_CODE"] = AUTHORIZATION_CODE
os.environ["CLIENT_ID"] = CLIENT_ID
os.environ["CLIENT_SECRET"] = CLIENT_SECRET
os.environ["REDIRECT_URI"] = REDIRECT_URI
os.environ["REFRESH_TOKEN"] = REFRESH_TOKEN
os.environ["RESTAURANT_ID"] = RESTAURANT_ID
os.environ["EXPIRES_IN"] = str(data["expires_in"])
os.environ["EXPIRES_AT"] = str(datetime.now() + timedelta(seconds=int(data["expires_in"])))

# export to .env file
with open('.env', 'w') as f:
    for key, value in os.environ.items():
        f.write(f"{key}={value}\n")