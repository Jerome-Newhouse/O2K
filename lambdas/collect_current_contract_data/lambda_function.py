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
                        "statusCode": 404,
                        "message": "Could not retrieve secret",
                        "body": f"Could not retrieve secret {name}: {e}"
                    }
        return {
            "statusCode": 200,
            "message": "Secrets retrieved successfully",
            "secrets": secrets
        }
    except Exception as e:
        logging.error(f"Could not retrieve secrets: {e}")
        return {
            "statusCode": 404,
            "message": "Could not retrieve secrets",
            "body": f"Could not retrieve secrets: {e}"
        }
    
def save_to_s3(data, bucket_name, prefix):
    try:
            
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}{datetime.now().strftime('%Y-%m-%d')}.json"
        json_data = json.dumps(data)
        s3.put_object(Bucket=bucket_name, Key=path, Body=json_data, ContentType='application/json')
        return {
            "statusCode": 200,
            "message": "Saved to S3",
            "body": f"Saved to S3: {path}"
        }
    except Exception as e:
        logging.error(f"Could not save to S3: {e}")
        return {
            "statusCode": 404,
            "message": "Could not save to S3",
            "body": f"Could not save to S3: {e}"
        }


    
    
    
    
def get_contract_data(secret):
    try:
        url = f"https://puckpedia.com/api/v2/players?api_key={secret}" 

        response = requests.get(url)
        return {
            "statusCode": 200,
            "message": "Contract data retrieved successfully",
            "body": response.json()
        }
    except Exception as e:
        logging.error(f"Could not get contract data: {e}")
        return {
            "statusCode": 404,
            "message": "Could not get contract data",
            "body": f"Could not get contract data: {e}"
        }
    

    
def lambda_handler(event, context):
    try:
        secrets = get_secrets()
        if secrets['statusCode'] == 200:
            contract_data = get_contract_data(secrets['secrets']['PuckPedia']['PuckPedia'])
            
            if contract_data['statusCode'] == 200:
                response = save_to_s3(contract_data['body'], event['bucket_name'], event['prefix'])
                
                if response['statusCode'] == 200:
                    return {
                        "statusCode": 200,
                        "message": "Contract data saved to S3",
                        "body": "Contract data saved to S3"
                    }
                else:
                    return {
                        "statusCode": 404,
                        "message": "Could not save contract data to S3",
                        "body": "Could not save contract data to S3"
                    }
            else:
                return {
                    "statusCode": 404,
                    "message": "Could not get contract data",
                    "body": "Could not get contract data"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Could not get secrets",
                "body": "Could not get secrets"
            }
            

    except Exception as e:
        logging.error(f"Could not get contract data: {e}")
        return {
            "statusCode": 404,
            "message": "Could not get contract data",
            "body": f"Could not get contract data: {e}"
        }

event = {
    "bucket_name": "puckpedia",
    "prefix": "players/current_contracts/",
    "key": "contract-data.json"
}
lambda_handler(event, None)