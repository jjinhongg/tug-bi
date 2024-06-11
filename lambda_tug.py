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

# Function to get all orders
def get_all_orders(location_id, all=True, update_at_min=None, create_at_min=None, create_at_max=None, page=1):
    page = 1
    orders = []
    
    # If no update_at_min is provided, get all orders
    if all:
        try:
            while True:
                url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/orders?location_id={location_id}&page={page}"
                response = requests.get(url, headers=aheaders)
                data = response.json()["data"]
                
                if not data:
                    break
                
                orders.extend(data)
                page += 1
                
        except Exception as e:
            print(f"An error occurred: {str(e)}")

    # If update_at_min is provided, get orders updated after the specified time
    # Update + sign with %2B for update_at_min
    if update_at_min:
        update_at_min = update_at_min.replace("+", "%2B")
        while True:
            url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/orders?location_id={location_id}&update_at_min={update_at_min}&page={page}"
            response = requests.get(url, headers=aheaders)
            data = response.json()["data"]
            
            if not data:
                break
            
            orders.extend(data)
            page += 1

    # If create_at_min is provided, get orders created after the specified time along with create_at_max
    if create_at_min:
        # add 32 days to create_at_min
        if not create_at_max:
            create_at_min = convert_to_datetime(create_at_min)
            create_at_max = create_at_min + timedelta(days=31)
        else:
            create_at_min = convert_to_datetime(create_at_min)
            create_at_max = convert_to_datetime(create_at_max)
            
        create_at_min = create_at_min.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        create_at_max = create_at_max.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        create_at_max = create_at_max.replace("+", "%2B")
        create_at_min = create_at_min.replace("+", "%2B")
        while True:
            url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/orders?location_id={location_id}&create_at_min={create_at_min}&create_at_max={create_at_max}&page={page}"
            response = requests.get(url, headers=aheaders)
            data = response.json()["data"]
            
            if not data:
                break
            
            orders.extend(data)
            page += 1
    
    return orders

def get_order_details(order_id, location='tug'):
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/orders/{order_id}"
    response = requests.get(url, headers=aheaders)
    order_details = response.json()["data"]
    order_details['location'] = location
    return order_details

# Function to upload to S3
def upload_data_to_s3(data, bucket_name, prefix, date_format="%Y-%m-%d"):
    if not data:
        logging.info(f"No data to upload for {prefix}.")
        return None

    last_created = convert_to_datetime(data[-1]['created_at']) + timedelta(seconds=1)
    file_key = f"{prefix}_{last_created.strftime(date_format)}.json"
    
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

    return last_created_str



def lambda_handler(event, context):
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

    locations = {
        'EVENT': params['EVENT'],
        'TUG': params['TUG']
    }

    last_created_TUG = params.get('LAST_CREATED_TUG')
    last_created_EVENT = params.get('LAST_CREATED_EVENT')

    bucket_name = 'tug-dinlr'
    all_order_details = []

    for location_name, location_id in locations.items():
        if "event" in location_name.lower():
            orders = get_all_orders(location_id, all=False, create_at_min=last_created_EVENT)
            order_details = [get_order_details(order["id"], location="event") for order in orders]
            last_created_EVENT = upload_data_to_s3(order_details, bucket_name, 'raw/EVENT_orders')
        elif "tug" in location_name.lower():
            orders = get_all_orders(location_id, all=False, create_at_min=last_created_TUG)
            order_details = [get_order_details(order["id"], location="tug") for order in orders]
            last_created_TUG = upload_data_to_s3(order_details, bucket_name, 'raw/TUG_orders')
        else:
            pass

    # Update SSM with the new last created timestamps
    if last_created_TUG:
        ssm_client.put_parameter(Name='/tug-dinlr/api/LAST_CREATED_TUG', Value=last_created_TUG, Type='String', Overwrite=True)
    if last_created_EVENT:
        ssm_client.put_parameter(Name='/tug-dinlr/api/LAST_CREATED_EVENT', Value=last_created_EVENT, Type='String', Overwrite=True)

    return {
        'statusCode': 200,
        'body': json.dumps('S3 put successful')
    }
