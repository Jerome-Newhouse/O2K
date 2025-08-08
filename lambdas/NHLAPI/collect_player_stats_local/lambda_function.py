import sys
import os
import requests
import logging
import boto3
import json
import pandas as pd
import tqdm
from io import StringIO

def get_player_stats(player_id):
    try:
        logging.info(f"Collecting player stats for player {player_id}")
        url = f"https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&cayenneExp=playerId={player_id}"
        response = requests.get(url)
        data = response.json()
        if data['total'] > 0:
            data = data['data']
            df = pd.DataFrame(data)
            return {
                "statusCode": 200,
                "message": "Player stats collected successfully",
                "body": df
            }
        else:
            return {
                "statusCode": 404,
                "message": "No player stats found",
                "body": f"No player stats found for player {player_id}"
            }
        
    except Exception as e:  
        return {
            "statusCode": 404,
            "message": "Could not collect player stats",
            "body": f"Could not collect player stats: {e}"
        }
        
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


def save_to_s3(data, bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}player_stats.csv"
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
        nhl_ids = get_nhl_ids(event['bucket_name'], event['nhl_ids_prefix'])
        if nhl_ids['statusCode'] == 200:
            nhl_ids = nhl_ids['body']
            player_stats_list = []
          
            for player_id in tqdm.tqdm(nhl_ids):
                player_stats = get_player_stats(player_id)
                if player_stats['statusCode'] == 200:
                    player_stats_list.append(player_stats['body'])
                
            df = pd.concat(player_stats_list)
            save_to_s3_response = save_to_s3(df, event['player_stats_bucket_name'], event['player_stats_prefix'])
            if save_to_s3_response['statusCode'] == 200:
                return {
                    "statusCode": 200,
                    "message": "Player stats collected successfully",
                    "body": "Player stats collected successfully"
                }
            else:
                return {
                    "statusCode": 404,
                    "message": "Could not save player stats to S3",
                    "body": "Could not save player stats to S3"
                }
        else:
            return {    
                "statusCode": 404,
                "message": "Could not retrieve NHL IDs",
                "body": "Could not retrieve NHL IDs"
            }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not collect player stats or retrieve NHL IDs",
            "body": f"Could not collect player stats or retrieve NHL IDs: {e}"
        }
        
        

event = {
    "bucket_name": "puckpedia",
    "player_stats_bucket_name": "nhlapi-data",
    "nhl_ids_prefix": "players/nhl_ids/nhl_ids.json",
    "player_stats_prefix": "players/player_stats/"
}
print(lambda_handler(event, None))