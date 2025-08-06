import boto3
import logging
import pandas as pd
from io import StringIO


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
        
        
def get_player_ids(csv_data):
    try:
        logging.info(f"Retrieving player IDs from CSV")
        player_ids = csv_data['player_id'].unique().tolist()
        logging.info(f"Player IDs retrieved successfully")
        return {
            "statusCode": 200,
            "message": "Player IDs retrieved successfully",
            "body": player_ids
        }   
    except Exception as e:
        logging.error(f"Could not retrieve player IDs from CSV: {e}")
        return {
            "statusCode": 404,
            "message": "Could not retrieve player IDs",
            "body": f"Could not retrieve player IDs: {e}"
        }
    


def lambda_handler(event, context):
    try:
        historical_contracts_csv_data = get_historical_contracts_csv_from_s3(event['bucket_name'], event['historical_contracts_prefix'])
        
        if historical_contracts_csv_data['statusCode'] == 200:
            current_contracts_csv_data = get_current_contracts_csv_from_s3(event['bucket_name'], event['current_contracts_prefix'])
            
            if current_contracts_csv_data['statusCode'] == 200:
                historical_contracts_csv_data['body'] = pd.concat([historical_contracts_csv_data['body'], current_contracts_csv_data['body']])
                player_ids = get_player_ids(historical_contracts_csv_data['body'])
                
                if player_ids['statusCode'] == 200:
                    return {
                        "statusCode": 200,
                        "message": "Player IDs retrieved successfully",
                        "body": player_ids['body']
                    }
                else:
                    return {
                        "statusCode": 404,
                        "message": "Could not retrieve player IDs",
                        
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
            "message": "Could not retrieve player IDs",
            "body": f"Could not retrieve player IDs: {e}"
        }
        
        
        
        
event = {
    "bucket_name": "puckpedia",
    "historical_contracts_prefix": "players/historical_contracts/historical_contracts.csv",
    "current_contracts_prefix": "players/current_contracts/current_contracts.csv"
}

response = lambda_handler(event, None)
print(len(response['body']))
print(response)
