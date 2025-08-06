import boto3
import json
import requests
import logging
from io import StringIO
import pandas as pd

def get_secrets():
    try:
        logging.info(f"Retrieving secrets")
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
        logging.info(f"Secrets retrieved successfully")
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
        logging.info(f"Saving to S3 for bucket {bucket_name} and prefix {prefix}")
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}current_contracts.csv"
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)  
        s3.put_object(Bucket=bucket_name, Key=path, Body=csv_buffer.getvalue(), ContentType='text/csv')
        logging.info(f"Saved to S3 for bucket {bucket_name} and prefix {prefix}")
            
        
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
        logging.info(f"Getting contract data")
        url = f"https://puckpedia.com/api/v2/players?api_key={secret}" 
        response = requests.get(url)
        logging.info(f"Contract data retrieved successfully")
        
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


def process_current_contract_data(data):
    try:
        logging.info(f"Processing current contract data")
        rows = []
        for player in data:
            player_info = {k: v for k, v in player.items() if k != 'current'}
            for contract in player.get('current', []):
                contract_info = {k: v for k, v in contract.items() if k != 'years'}
                
                for year in contract.get('years', []):
                    # Merge player info + contract info + year
                    row = {**player_info, **contract_info, **year}
                    rows.append(row)

        
        df = pd.DataFrame(rows)
        logging.info(f"Current contract data processed successfully")
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
    try:
        secrets = get_secrets()
        if secrets['statusCode'] == 200:   
            contract_data = get_contract_data(secrets['secrets']['PuckPedia']['PuckPedia'])

            if contract_data['statusCode'] == 200:
                processed_contract_data = process_current_contract_data(contract_data['body'])
                
                if processed_contract_data['statusCode'] == 200:
                    response = save_to_s3(processed_contract_data['body'], event['bucket_name'], event['prefix'])
                    
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
                        "message": "Could not process contract data",
                        "body": "Could not process contract data"
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