import boto3
import pandas as pd
import json
import os
import sys
import logging
import requests
import re
import time
from io import StringIO
import tqdm 

def get_goalie_stats(bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        data = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(data))
        return {
            "statusCode": 200,
            "message": "Goalie stats retrieved successfully",
            "body": data
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not retrieve goalie stats",
            "body": f"Could not retrieve goalie stats: {e}"
        }
    
def get_contracts(bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        data = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(data))
        return {
            "statusCode": 200,
            "message": "Contracts retrieved successfully",
            "body": data
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not retrieve contracts",
            "body": f"Could not retrieve contracts: {e}"
        }
    
def merge_goalie_stats_contracts(goalie_stats, contracts):
    try:
        print(goalie_stats)
        print(contracts)
        merged_df = pd.merge(goalie_stats, contracts, left_on=['playerId', 'seasonId'], right_on=['nhl_id', 'season'], how='inner')
        return {
            "statusCode": 200,
            "message": "Goalie stats and contracts merged successfully",
            "body": merged_df
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not merge goalie stats and contracts",
            "body": f"Could not merge goalie stats and contracts: {e}"
        }
    
def save_to_s3(data, bucket_name, prefix):      
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}goalie_stats_contracts.csv"
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=path, Body=csv_buffer.getvalue())
        return {
            "statusCode": 200,
            "message": "Saved to S3",
            "body": "Saved to S3"
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not save to S3",
            "body": f"Could not save to S3: {e}"
        }



def lambda_handler(event, context):
    try:
        goalie_stats = get_goalie_stats(event['bucket_name'], event['goalie_stats_prefix'])
        if goalie_stats['statusCode'] == 200:
            current_contracts = get_contracts(event['contracts_bucket_name'], event['player_current_contracts_prefix'])
            historical_contracts = get_contracts(event['contracts_bucket_name'], event['player_historical_contracts_prefix'])
            if current_contracts['statusCode'] == 200 and historical_contracts['statusCode'] == 200:
                current_contracts = current_contracts['body']
                historical_contracts = historical_contracts['body']
                merged_contracts = pd.concat([current_contracts, historical_contracts])
                merged_contracts['season'] = merged_contracts['season'].astype(str).str.replace('-', '', regex=False).astype(int)
                
                merged_stats = merge_goalie_stats_contracts(goalie_stats['body'], merged_contracts)
                if merged_stats['statusCode'] == 200:   
                    save_to_s3_response = save_to_s3(merged_stats['body'], event['merged_stats_bucket_name'], event['merged_stats_prefix'])
                    if save_to_s3_response['statusCode'] == 200:
                        return {
                            "statusCode": 200,
                            "message": "Merge goalie stats contracts",
                            "body": "Merge goalie stats contracts"
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
                        "message": "Could not merge goalie stats and contracts",
                        "body": f"Could not merge goalie stats and contracts: {merged_stats['body']}"
                    }
            else:
                return {
                    "statusCode": 404,
                    "message": "Could not retrieve contracts",
                    "body": f"Could not retrieve contracts: {contracts['body']}"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Could not retrieve goalie stats",
                "body": f"Could not retrieve goalie stats: {goalie_stats['body']}"
            }
    except Exception as e:
        return {
            "statusCode": 500,
            "message": "Error merging goalie stats contracts",
            "body": f"Error merging goalie stats contracts: {e}"
        }
        
        
event = {
    "bucket_name": "nhlapi-data",
    "goalie_stats_prefix": "players/player_stats/goalie_stats.csv",
    "merged_stats_prefix": "players/merged_data/merged_goalie_stats_contracts.csv",
    "merged_stats_bucket_name": "puckpedia",
    "contracts_bucket_name": "puckpedia",
    "player_current_contracts_prefix": "players/current_contracts/current_contracts.csv",
    "player_historical_contracts_prefix": "players/historical_contracts/historical_contracts.csv",
}

print(lambda_handler(event, None))