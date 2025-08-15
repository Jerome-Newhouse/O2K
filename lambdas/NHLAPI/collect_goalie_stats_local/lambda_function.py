import boto3
import json
import requests
import pandas as pd
from io import StringIO
import tqdm

def get_nhl_ids(bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        data = response['Body'].read().decode('utf-8')
        data = json.loads(data)
        
        return {
            "statusCode": 200,
            "message": "NHL IDs retrieved successfully",
            "body": data['nhl_ids']
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not retrieve NHL IDs",
            "body": f"Could not retrieve NHL IDs: {e}"
        }

def get_goalie_stats(goalie_id):
    try:
        url = f"https://api.nhle.com/stats/rest/en/goalie/summary?limit=-1&cayenneExp=playerId={goalie_id}"
        response = requests.get(url)
        data = response.json()
        if data['total'] > 0:
            data = data['data']
            df = pd.DataFrame(data)
            return {
                "statusCode": 200,
                "message": "Goalie stats retrieved successfully",
                "body": df
            }
        else:
            return {
                "statusCode": 404,
                "message": "No goalie stats found",
                "body": f"No goalie stats found for goalie {goalie_id}"
            }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not retrieve goalie stats",
            "body": f"Could not retrieve goalie stats: {e}"
        }

def save_to_s3(data, bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}goalie_stats.csv"
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=path, Body=csv_buffer.getvalue())
        return {
            "statusCode": 200,
            "message": "Saved to S3",
            "body": "Saved to S3"
        }
    except Exception as e:
        print(f"Could not save to S3: {e}")
        return {
            "statusCode": 404,
            "message": "Could not save to S3",
            "body": f"Could not save to S3: {e}"
        }

def lambda_handler(event, context):
    try:
        goalie_ids = get_nhl_ids(event['bucket_name'], event['nhl_ids_prefix'])
        if goalie_ids['statusCode'] == 200:
            goalie_stats_list = []
            for goalie_id in tqdm.tqdm(goalie_ids['body']):
                goalie_stats = get_goalie_stats(goalie_id)
                if goalie_stats['statusCode'] == 200:
                    goalie_stats_list.append(goalie_stats['body'])
            df = pd.concat(goalie_stats_list)
            save_to_s3_response = save_to_s3(df, event['player_stats_bucket_name'], event['player_stats_prefix'])
            if save_to_s3_response['statusCode'] == 200:
                return {
                    "statusCode": 200,
                    "message": "Goalie stats saved to S3",
                    "body": "Goalie stats saved to S3"
                }
            else:
                return {
                    "statusCode": 404,  
                    "message": "Could not save to S3",
                    "body": f"Could not save to S3: {save_to_s3_response['body']}"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Could not retrieve goalie IDs",
                "body": f"Could not retrieve goalie IDs: {goalie_ids['body']}"
            }
    except Exception as e:
        return {
            "statusCode": 500,
            "message": "Error retrieving goalie IDs",
            "body": f"Error retrieving goalie IDs: {e}"
        }
    
    
    
    
    
    
event = {
    "bucket_name": "puckpedia",
    "player_stats_bucket_name": "nhlapi-data",
    "nhl_ids_prefix": "players/nhl_ids/nhl_ids.json",
    "player_stats_prefix": "players/player_stats/"
}

print(lambda_handler(event, None))