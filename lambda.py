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
def fetch_ssm_parameters(path):
    response = ssm_client.get_parameters_by_path(Path=path, Recursive=True, WithDecryption=True)
    return {param['Name'].split('/')[-1]: param['Value'] for param in response['Parameters']}

params = fetch_ssm_parameters('/tug-dinlr/api/')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API configuration
base_url = "https://api.dinlr.com/v1"
aheaders = {"Authorization": f"Bearer {params['ACCESS_TOKEN']}"}
rheaders = {'Content-Type': 'application/x-www-form-urlencoded'}

def get_locations(restaurant_id, headers):
    """Fetch locations from the API and return an iterable of (id, name)."""
    response = requests.get(f"{base_url}/{restaurant_id}/onlineorder/locations", headers=headers)
    response.raise_for_status()  # Ensure we raise an error for bad responses
    data = response.json()
    return [(location['id'], location['name']) for location in data['data']]

locations = get_locations(params['RESTAURANT_ID'], aheaders)

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
    response.raise_for_status()  # Ensure we raise an error for bad responses
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

def get_all_orders(location_id, all=True, update_at_min=None, create_at_min=None, create_at_max=None):
    orders = []
    page = 1
    while True:
        url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/orders?location_id={location_id}&page={page}"
        
        if update_at_min:
            update_at_min = update_at_min.replace("+", "%2B")
            url += f"&update_at_min={update_at_min}"
        
        if create_at_min:
            create_at_min = convert_to_datetime(create_at_min).strftime("%Y-%m-%dT%H:%M:%S+08:00").replace("+", "%2B")
            create_at_max = convert_to_datetime(create_at_max).strftime("%Y-%m-%dT%H:%M:%S+08:00").replace("+", "%2B") if create_at_max else None
            url += f"&create_at_min={create_at_min}"
            if create_at_max:
                url += f"&create_at_max={create_at_max}"

        response = requests.get(url, headers=aheaders)
        response.raise_for_status()
        data = response.json()["data"]

        if not data:
            break

        orders.extend(data)
        page += 1

    return orders

def get_order_details(order_id, location='tug'):
    url = f"{base_url}/{params['RESTAURANT_ID']}/onlineorder/orders/{order_id}"
    response = requests.get(url, headers=aheaders)
    response.raise_for_status()
    order_details = response.json()["data"]
    order_details['location'] = location
    return order_details

def upload_data_to_s3(data, bucket_name, prefix, date_format="%Y-%m-%d"):
    if not data:
        logging.info(f"No data to upload for {prefix}.")
        return None

    try:
        last_created = convert_to_datetime(data[-1]['created_at']) + timedelta(seconds=1)
        last_created_str = last_created.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        file_key = f"{prefix}_{datetime.now().strftime(date_format)}.json"

        s3.Object(bucket_name, file_key).put(Body=(bytes(json.dumps(data, indent=4).encode('UTF-8'))))
        logging.info(f"Successfully uploaded {prefix} data to S3.")
        return last_created_str
    except Exception as e:
        logging.error(f"Failed to upload {prefix} data: {e}")
        return None

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

    last_created_TUG = params.get('LAST_CREATED_TUG')
    last_created_BANGSAR = params.get('LAST_CREATED_BANGSAR')
    last_created_EVENT = params.get('LAST_CREATED_EVENT')

    bucket_name = 'tug-dinlr'
    all_order_details = []

    for location_id, location_name in locations:
        if "event" in location_name.lower():
            orders = get_all_orders(location_id, all=False, create_at_min=last_created_EVENT)
            order_details = [get_order_details(order["id"], location="event") for order in orders]
            last_created_EVENT = upload_data_to_s3(order_details, bucket_name, 'raw/EVENT_orders')
        elif "tug gelato" in location_name.lower():
            orders = get_all_orders(location_id, all=False, create_at_min=last_created_TUG)
            order_details = [get_order_details(order["id"], location="tug") for order in orders]
            last_created_TUG = upload_data_to_s3(order_details, bucket_name, 'raw/TUG_orders')
        elif "tug @ bangsar" in location_name.lower():
            orders = get_all_orders(location_id, all=False, create_at_min=last_created_TUG)
            order_details = [get_order_details(order["id"], location="tug_Bangsar") for order in orders]
            last_created_BANGSAR = upload_data_to_s3(order_details, bucket_name, 'raw/TUG_Bangsar_orders')
        else:
            # If there are other locations that need to be processed differently, handle them here
            logging.info(f"Unknown location type: {location_name}")

    # Update SSM with the new last created timestamps
    if last_created_TUG:
        ssm_client.put_parameter(Name='/tug-dinlr/api/LAST_CREATED_TUG', Value=last_created_TUG, Type='String', Overwrite=True)
    if last_created_BANGSAR:
        ssm_client.put_parameter(Name='/tug-dinlr/api/LAST_CREATED_BANGSAR', Value=last_created_EVENT, Type='String', Overwrite=True)
    if last_created_EVENT:
        ssm_client.put_parameter(Name='/tug-dinlr/api/LAST_CREATED_EVENT', Value=last_created_EVENT, Type='String', Overwrite=True)

    return {
        'statusCode': 200,
        'body': json.dumps('S3 put successful')
    }
