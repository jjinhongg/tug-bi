import requests
import json
import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Define the parameters for the POST request
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI")
RESTAURANT_ID = os.environ.get("RESTAURANT_ID")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

# Step 3: Use the access token to interact with the Dinlr API
# For example, to get a list of all locations of the restaurant
headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

# Send the GET request to the locations endpoint
response = requests.get(f"https://api.dinlr.com/v1/{RESTAURANT_ID}/onlineorder/locations", headers=headers)

# Parse the JSON response and print the location names
data = response.json()
locations = data["data"]
for location in locations:
    print(location["name"])

# # Step 4: Request a new access token using the refresh token
# # Define the parameters for the POST request
# params = {
#     "grant_type": "refresh_token",
#     "client_id": client_id,
#     "client_secret": client_secret,
#     "refresh_token": refresh_token
# }

# # Send the POST request to the token endpoint
# response = requests.post(f"https://api.dinlr.com/v1/{restaurant_id}/oauth/token", data=params)

# # Parse the JSON response and extract the new access token
# data = response.json()
# access_token = data["access_token"]

# Use the new access token to interact with the Dinlr API as before

# To implement CI/CD, you can use a tool like GitHub Actions to automate the testing and deployment of your code
# For example, you can create a workflow file in your repository that runs the following steps:

# # Step 1: Checkout the code from the repository
# - name: Checkout
#   uses: actions/checkout@v2

# # Step 2: Install the requests module
# - name: Install requests
#   run: pip install requests

# # Step 3: Run the code and check for errors
# - name: Run code
#   run: python dinlr_api.py

# # Step 4: Deploy the code to a server or a cloud platform
# - name: Deploy code
#   # Use a suitable action or script to deploy your code, depending on your target platform
#   # For example, if you are using AWS Lambda, you can use the following action:
#   uses: appleboy/lambda-action@master
#   with:
#     aws_access_key_id: ${{ secrets.AWS_ACCESS_KEY_ID }}
#     aws_secret_access_key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
#     aws_region: us-east-1
#     function_name: dinlr_api
#     zip_file: dinlr_api.zip
