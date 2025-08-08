import boto3
import logging
import pandas as pd
from io import StringIO
import json


def get_historical_contracts_csv_from_s3(bucket_name, prefix):
    try:
        logging.info(f"Retrieving CSV from S3 for bucket {bucket_name} and prefix {prefix}")
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)    
        csv_data = response['Body'].read().decode('utf-8')
        csv_data = pd.read_csv(StringIO(csv_data))
        logging.info(f"CSV retrieved successfully for bucket {bucket_name} and prefix {prefix}")
    
        return {
            "statusCode": 200,
            "message": "CSV retrieved successfully",
            "body": csv_data
        }
    except Exception as e:
        logging.error(f"Could not retrieve CSV from S3 for bucket {bucket_name} and prefix {prefix}: {e}")
        return {
            "statusCode": 404,
            "message": "Could not retrieve CSV",
            "body": f"Could not retrieve CSV: {e}"
        }
        
def get_current_contracts_csv_from_s3(bucket_name, prefix):
    try:
        logging.info(f"Retrieving CSV from S3 for bucket {bucket_name} and prefix {prefix}")
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        csv_data = response['Body'].read().decode('utf-8')
        csv_data = pd.read_csv(StringIO(csv_data))
        logging.info(f"CSV retrieved successfully for bucket {bucket_name} and prefix {prefix}")
        return {
            "statusCode": 200,
            "message": "CSV retrieved successfully",
            "body": csv_data
        }
    except Exception as e:
        logging.error(f"Could not retrieve CSV from S3 for bucket {bucket_name} and prefix {prefix}: {e}")
        return {
            "statusCode": 404,
            "message": "Could not retrieve CSV",
            "body": f"Could not retrieve CSV: {e}"
        }
        
        
def get_nhl_ids(csv_data):
    try:
        logging.info(f"Retrieving NHL IDs from CSV")
        nhl_ids = csv_data['nhl_id'].unique().tolist()
        logging.info(f"NHL IDs retrieved successfully")
        return {
            "statusCode": 200,
            "message": "NHL IDs retrieved successfully",
            "body": nhl_ids
        }   
    except Exception as e:
        logging.error(f"Could not retrieve NHL IDs from CSV: {e}")   
        return {
            "statusCode": 404,
            "message": "Could not retrieve NHL IDs",
            "body": f"Could not retrieve player IDs: {e}"
        }
    

def save_to_s3(data, bucket_name, prefix):
    try:
        logging.info(f"Saving to S3 for bucket {bucket_name} and prefix {prefix}")
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}nhl_ids.json"
        data = {
            "nhl_ids": data
        }
        json_data = json.dumps(data)
        s3.put_object(Bucket=bucket_name, Key=path, Body=json_data, ContentType='application/json')
        logging.info(f"Saved to S3 for bucket {bucket_name} and prefix {prefix}")
        return {
            "statusCode": 200,
            "message": "Saved to S3",
            "body": "Saved to S3"
        }
    except Exception as e:      
        logging.error(f"Could not save to S3 for bucket {bucket_name} and prefix {prefix}: {e}")
        return {
            "statusCode": 404,
            "message": "Could not save to S3",
            "body": f"Could not save to S3: {e}"
        }

def lambda_handler(event, context):
    try:
        historical_contracts_csv_data = get_historical_contracts_csv_from_s3(event['bucket_name'], event['historical_contracts_prefix'])
        
        if historical_contracts_csv_data['statusCode'] == 200:
            current_contracts_csv_data = get_current_contracts_csv_from_s3(event['bucket_name'], event['current_contracts_prefix'])
            
            if current_contracts_csv_data['statusCode'] == 200:
                historical_contracts_csv_data['body'] = pd.concat([historical_contracts_csv_data['body'], current_contracts_csv_data['body']])
                nhl_ids = get_nhl_ids(historical_contracts_csv_data['body'])
                
                if nhl_ids['statusCode'] == 200:
                    save_to_s3(nhl_ids['body'], event['bucket_name'], event['nhl_ids_prefix'])
                    return {
                        "statusCode": 200,
                        "message": "Player IDs retrieved successfully",
                        "body": nhl_ids['body']
                    }
                else:
                    return {
                        "statusCode": 404,
                        "message": "Could not retrieve NHL IDs",
                        
                    }
            else:           
                return {
                    "statusCode": 404,
                    "message": "Could not retrieve current contracts CSV",
                    
                }
        else:
            return {
                "statusCode": 404,
                "message": "Could not retrieve historical contracts CSV",
                
            }
   
 
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not retrieve NHL IDs",
            "body": f"Could not retrieve NHL IDs: {e}"
        }
        
        
        
        
event = {
    "bucket_name": "puckpedia",
    "historical_contracts_prefix": "players/historical_contracts/historical_contracts.csv",
    "current_contracts_prefix": "players/current_contracts/current_contracts.csv",
    "nhl_ids_prefix": "players/nhl_ids/"
}

response = lambda_handler(event, None)
print(len(response['body']))
print(response)
