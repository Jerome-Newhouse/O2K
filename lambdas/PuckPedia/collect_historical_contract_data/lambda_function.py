import boto3
import json
import requests
from datetime import datetime
import time
import logging
import pandas as pd
from io import StringIO




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
        
        
def get_historical_contract_data(secrets):
    try:
        url = f"https://puckpedia.com/api/v2/players?api_key={secrets['PuckPedia']['PuckPedia']}&contract_type=history"
        response = requests.get(url)
        return {
            "statusCode": 200,
            "message": "Historical contract data retrieved successfully",
            "body": response.json()
        }
    except Exception as e:
        logging.error(f"Could not get historical contract data: {e}")
        return {
            "statusCode": 404,
            "message": "Could not get historical contract data",
            "body": f"Could not get historical contract data: {e}"
        }
        
def save_to_s3(data, bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}historical_contracts.csv"
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)  
        s3.put_object(Bucket=bucket_name, Key=path, Body=csv_buffer.getvalue(), ContentType='text/csv')

        return {
            "statusCode": 200,
            "message": "Historical contract data saved to S3",
            "body": f"Saved to S3: {path}"
        }
    except Exception as e:
        logging.error(f"Could not save to S3: {e}")
        return {
            "statusCode": 404,
            "message": "Could not save to S3",
            "body": f"Could not save to S3: {e}"
        }
        
        
        
def process_historical_contract_data(data):
    try:
        rows = []
        for player in data:
            player_info = {k: v for k, v in player.items() if k != 'history'}
            for contract in player.get('history', []):
                contract_info = {k: v for k, v in contract.items() if k != 'years'}
                
                for year in contract.get('years', []):
                    # Merge player info + contract info + year
                    row = {**player_info, **contract_info, **year}
                    rows.append(row)

        # Create DataFrame
        df = pd.DataFrame(rows)
        return {
            "statusCode": 200,
            "message": "Historical contract data processed successfully",
            "body": df
        }
       
            
    except Exception as e:
        logging.error(f"Could not process historical contract data: {e}")
        return {
            "statusCode": 404,
            "message": "Could not process historical contract data",
            "body": f"Could not process historical contract data: {e}"
        }


        
def lambda_handler(event, context):
    secrets = get_secrets()
    if secrets['statusCode'] == 200:
        historical_contract_data = get_historical_contract_data(secrets['secrets'])
        processed_data = process_historical_contract_data(historical_contract_data['body'])
        if processed_data['statusCode'] == 200:
            response = save_to_s3(processed_data['body'], event['bucket_name'], event['prefix'])
            if response['statusCode'] == 200:
                return {
                    "statusCode": 200,
                    "message": "Historical contract data saved to S3",
                    "body": "Historical contract data saved to S3"
                }
            else:
                return {
                    "statusCode": 404,
                    "message": "Could not save historical contract data to S3",
                    "body": "Could not save historical contract data to S3"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Could not get historical contract data",
                "body": "Could not get historical contract data"
            }
    else:
        return {
            "statusCode": 404,
            "message": "Could not get secrets",
            "body": "Could not get secrets"
        }   
    
    
    
event = {
    "bucket_name": "puckpedia",
    "prefix": "players/historical_contracts/",
}
lambda_handler(event, None)