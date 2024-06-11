import json
import boto3
import requests
import logging
from datetime import datetime, timedelta, timezone

# Initialize AWS clients and timezone
ssm_client = boto3.client('ssm')
s3 = boto3.resource('s3')
utc_plus_8 = timezone(timedelta(hours=8))

# Fetch parameters from SSM
# Function to get secrets from AWS SSM
def get_secrets(path='/tug-dinlr/api/'):
    secrets = {}
    next_token = None
    
    while True:
        params = ssm_client.get_parameters_by_path(
            Path=path,
            Recursive=True,
            WithDecryption=True,
            NextToken=str(next_token) if next_token is not None else ''
        )
        
        for param in params['Parameters']:
            param_name = param['Name'].split('/')[-1]
            param_value = param['Value']
            secrets[param_name] = param_value
        
        if 'NextToken' in params:
            next_token = params['NextToken']
        else:
            break
    
    return secrets

params = get_secrets('/tug-dinlr/api/')

# API CONFIG
base_url = "https://api.dinlr.com/v1"
rheaders = {'Content-Type': 'application/x-www-form-urlencoded'}
aheaders = {"Authorization": f"Bearer {params['ACCESS_TOKEN']}"}

def get_locations(restaurant_id, headers):
    """Fetch locations from the API and return an iterable of (id, name)."""
    response = requests.get(f"{base_url}/{restaurant_id}/onlineorder/locations", headers=headers)
    data = response.json()
    return [(location['id'], location['name']) for location in data['data']]

def convert_to_datetime(date_string):
    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S+08:00")

def convert_to_datetime_timezone(date_string):
    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")

def is_token_expired(expiry_date_str):
    """Check if the access token has expired."""
    expiry_date = convert_to_datetime_timezone(expiry_date_str)
    return datetime.now(utc_plus_8) >= expiry_date

def refresh_access_token():
    """Request a new access token using the refresh token."""
    parameters = {
        "refresh_token": params['REFRESH_TOKEN'],
        "client_id": params['CLIENT_ID'],
        "client_secret": params['CLIENT_SECRET'],
        "grant_type": "refresh_token"
    }

    response = requests.post(f"{base_url}/{params['RESTAURANT_ID']}/oauth/token", data=parameters, headers=rheaders)
    data = response.json()

    new_params = {
        'ACCESS_TOKEN': data["access_token"],
        'REFRESH_TOKEN': data["refresh_token"],
        'EXPIRES_AT': (datetime.now(utc_plus_8) + timedelta(seconds=int(data["expires_in"]))).strftime("%Y-%m-%dT%H:%M:%S+08:00"),
        'EXPIRES_IN': str(data["expires_in"])
    }

    for key, value in new_params.items():
        ssm_client.put_parameter(Name=f'/tug-dinlr/api/{key}', Value=value, Type='String', Overwrite=True)

    return new_params

def get_items_dim(location_id):
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/items?location_id={location_id}"
    response = requests.get(url, headers=aheaders)
    items = response.json()["data"]
    return items

def get_categories_dim():
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/categories"
    response = requests.get(url, headers=aheaders)
    categories = response.json()["data"]
    return categories

def get_modifiers_dim(location_id):
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/modifiers?location_id={location_id}"
    response = requests.get(url, headers=aheaders)
    modifiers = response.json()["data"]
    return modifiers

def get_discounts_dim(location_id):
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/discounts?location_id={location_id}"
    response = requests.get(url, headers=aheaders)
    discounts = response.json()["data"]
    return discounts

def get_promotions_dim(location_id):
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/promotions?location_id={location_id}"
    response = requests.get(url, headers=aheaders)
    promotions = response.json()["data"]
    return promotions

def get_customers_dim():
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/customers"
    response = requests.get(url, headers=aheaders)
    customers = response.json()["data"]
    return customers

def get_vouchers_dim():
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/vouchers"
    response = requests.get(url, headers=aheaders)
    vouchers = response.json()["data"]
    return vouchers

# Function to upload to S3
def upload_data_to_s3(data, bucket_name, prefix, date_format="%Y-%m-%d"):
    if not data:
        logging.info(f"No data to upload for {prefix}.")
        return None

    file_key = f"dim_{prefix}.json"
    
    try:
        # Check if the file already exists in S3
        obj = s3.Object(bucket_name, file_key)
        try:
            existing_data = json.loads(obj.get()['Body'].read().decode('utf-8'))
            logging.info(f"Existing data found for {file_key}.")
        except obj.meta.client.exceptions.NoSuchKey:
            existing_data = []
            logging.info(f"No existing data found for {file_key}. Creating new file.")

        # Combine and deduplicate data using a dictionary keyed by a unique identifier
        combined_data_dict = {item['id']: item for item in existing_data}
        combined_data_dict.update({item['id']: item for item in data})
        
        # Convert back to list to maintain the order
        combined_data = list(combined_data_dict.values())

        last_created = convert_to_datetime(combined_data[-1]['created_at']) + timedelta(seconds=1)
        last_created_str = last_created.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        
        # Upload combined data back to S3
        obj.put(Body=(bytes(json.dumps(combined_data, indent=4).encode('UTF-8'))))
        logging.info(f"Successfully uploaded {prefix} data to S3.")
        
    except Exception as e:
        logging.error(f"Failed to upload {prefix} data: {e}")



def lambda_handler(context, event):
    if is_token_expired(params['EXPIRES_AT']):
        try:
            new_params = refresh_access_token()
            params.update(new_params)
        except Exception as e:
            logging.error(f"Failed to refresh token: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps('Token refresh failed')
            }

    bucket_name = 'tug-dinlr'

    # For each location, get dimensions and combine
    locations = get_locations(params['RESTAURANT_ID'], aheaders)
    for location_id, location_name in locations:
        items = get_items_dim(location_id)
        promotions = get_promotions_dim(location_id)
        discounts = get_discounts_dim(location_id)

        upload_data_to_s3(items, bucket_name, 'raw/items/items')
        upload_data_to_s3(promotions, bucket_name, 'raw/promotions/promotions')
        upload_data_to_s3(discounts, bucket_name, 'raw/discounts/discounts')
        
    customers = get_customers_dim()
    vouchers = get_vouchers_dim()
    upload_data_to_s3(customers, bucket_name, 'raw/customers/customers')
    upload_data_to_s3(vouchers, bucket_name, 'raw/vouchers/vouchers')

    return {
        'statusCode': 200,
        'body': json.dumps('S3 put successful')
    }
