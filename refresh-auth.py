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
REDIRECT_URI = os.environ.get("REDIRECT_URI")
RESTAURANT_ID = os.environ.get("RESTAURANT_ID")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
EXPIRES_IN = os.environ.get("EXPIRES_IN")
EXPIRES_AT = os.environ.get("EXPIRES_AT")

def is_token_expired():
    # Check if the access token has expired
    return datetime.now() >= EXPIRES_AT

try:
    if is_token_expired():
        # Step 4: Request a new access token using the refresh token
        # Define the parameters for the POST request
        params = {
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN
        }

        # Send the POST request to the token endpoint
        response = requests.post(f"https://api.dinlr.com/v1/{RESTAURANT_ID}/oauth/token", data=params)

        # Parse the JSON response and extract the new access token
        data = response.json()
        ACCESS_TOKEN = data["access_token"]
        REFRESH_TOKEN = data["refresh_token"]
        EXPIRES_IN = data["expires_in"]

        # Update the .env file with the new access token and refresh token
        os.environ["ACCESS_TOKEN"] = ACCESS_TOKEN
        os.environ["EXPIRES_IN"] = EXPIRES_IN
        os.environ["REFRESH_TOKEN"] = REFRESH_TOKEN

        # export to .env file
        with open('.env', 'w') as f:
            for key, value in os.environ.items():
                f.write(f"{key}={value}\n")

except Exception as e:
    # Handle any exceptions that may occur
    print(f"An error occurred: {str(e)}")

# Update the .env file with the new access token and refresh token
os.environ["ACCESS_TOKEN"] = ACCESS_TOKEN
os.environ["EXPIRES_IN"] = EXPIRES_IN
# Declare EXPIRES_AT environment variable
os.environ["EXPIRES_AT"] = str(datetime.now() + timedelta(seconds=int(EXPIRES_IN)))
os.environ["REFRESH_TOKEN"] = REFRESH_TOKEN

# export to .env file
with open('.env', 'w') as f:
    for key, value in os.environ.items():
        f.write(f"{key}={value}\n")
