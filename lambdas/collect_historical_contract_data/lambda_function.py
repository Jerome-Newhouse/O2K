import boto3
import json
import requests
from datetime import datetime
import time
import logging




def get_secrets():
    try:
        client = boto3.client("secretsmanager", region_name="us-east-2")
        secrets = {}

        paginator = client.get_paginator("list_secrets")
        for page in paginator.paginate():
            for secret in page["SecretList"]:
                name = secret["Name"]
                try:
                    response = client.get_secret_value(SecretId=name)
                    if "SecretString" in response:
                        secrets[name] = json.loads(response["SecretString"])
                    else:
                        secrets[name] = response["SecretBinary"]  # base64 encoded
                except Exception as e:
                    logging.error(f"Could not retrieve secret {name}: {e}")
        return {
            "statusCode": 200,
            "secrets": secrets
        }
    except Exception as e:
        logging.error(f"Could not retrieve secrets: {e}")
        return {
            "statusCode": 404,
            "body": f"Could not retrieve secrets: {e}"
        }
        
        
def get_historical_contract_data(secrets):
    try:
        url = f"https://puckpedia.com/api/v2/players?api_key={secrets['PuckPedia']['PuckPedia']}&contract_type=history"
        response = requests.get(url)
        return {
            "statusCode": 200,
            "body": response.json()
        }
    except Exception as e:
        logging.error(f"Could not get historical contract data: {e}")
        return {
            "statusCode": 404,
            "body": f"Could not get historical contract data: {e}"
        }
        
def save_to_s3(data, bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}{datetime.now().strftime('%Y-%m-%d')}.json"
        json_data = json.dumps(data)
        s3.put_object(Bucket=bucket_name, Key=path, Body=json_data, ContentType='application/json')
        return {
            "statusCode": 200,
            "body": f"Saved to S3: {path}"
        }
    except Exception as e:
        logging.error(f"Could not save to S3: {e}")
        return {
            "statusCode": 404,
            "body": f"Could not save to S3: {e}"
        }
        

        
def lambda_handler(event, context):
    secrets = get_secrets()
    print(secrets)
    historical_contract_data = get_historical_contract_data(secrets['secrets'])
    print(historical_contract_data)
    response = save_to_s3(historical_contract_data['body'], event['bucket_name'], event['prefix'])
    print(response)
    
    
event = {
    "bucket_name": "puckpedia",
    "prefix": "players/historical_contracts/",
}
lambda_handler(event, None)