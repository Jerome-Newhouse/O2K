import boto3
import pandas as pd
from io import StringIO


def get_player_stats_from_s3(bucket_name, prefix):

    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        data = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(data))
        return {
            "statusCode": 200,
            "message": "Player stats retrieved successfully",
            "body": data
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not retrieve player stats",
            "body": f"Could not retrieve player stats: {e}"
        }
    
    
def get_player_contracts_from_s3(bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        data = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(data))
        return {
            "statusCode": 200,
            "message": "Player contracts retrieved successfully",
            "body": data
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not retrieve player contracts",
            "body": f"Could not retrieve player contracts: {e}"
        }
    
def save_csv_to_s3(data, bucket_name, prefix):
    try:
        path = f"{prefix}merged_data.csv"
        s3 = boto3.client("s3", region_name="us-east-2")
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=path, Body=csv_buffer.getvalue())
        return {
            "statusCode": 200,
            "message": "Merged data saved to S3",
            "body": "Merged data saved to S3"
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not save merged data to S3",
            "body": f"Could not save merged data to S3: {e}"
        }
        


def lambda_handler(event, context):
    try:
        player_stats = get_player_stats_from_s3(event['player_stats_bucket_name'], event['player_stats_prefix'])
        if player_stats['statusCode'] == 200:
            current_player_contracts = get_player_contracts_from_s3(event['player_contracts_bucket_name'], event['player_current_contracts_prefix'])
            if current_player_contracts['statusCode'] == 200:
                current_player_contracts['body']['season'] = current_player_contracts['body']['season'].astype(str).str.replace('-', '', regex=False).astype(int)
                historical_player_contracts = get_player_contracts_from_s3(event['player_contracts_bucket_name'], event['player_historical_contracts_prefix'])
                if historical_player_contracts['statusCode'] == 200:
                    historical_player_contracts['body']['season'] = historical_player_contracts['body']['season'].astype(str).str.replace('-', '', regex=False).astype(int)
                    player_contracts = pd.concat([current_player_contracts['body'], historical_player_contracts['body']])
                    merged_data = pd.merge(player_stats['body'], player_contracts, left_on=['playerId', 'seasonId'], right_on=['nhl_id', 'season'], how='inner')
                    save_csv_to_s3_response = save_csv_to_s3(merged_data, event['merged_data_bucket_name'], event['merged_data_prefix'])
                    
                    if save_csv_to_s3_response['statusCode'] == 200:
                        return {
                            "statusCode": 200,
                            "message": "Stats and contracts merged successfully",
                            "body": "Stats and contracts merged successfully"
                        }
                    else:
                        return {
                            "statusCode": 404,
                            "message": "Could not save merged data to S3",
                            "body": "Could not save merged data to S3"
                        }
                else:
                    return {
                        "statusCode": 404,
                        "message": "Could not retrieve historical player contracts",
                        "body": "Could not retrieve historical player contracts"
                    }
            else:
                return {
                    "statusCode": 404,
                    "message": "Could not retrieve current player contracts",
                    "body": "Could not retrieve current player contracts"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Could not retrieve player stats",
                "body": "Could not retrieve player stats"
            }
    
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not merge stats and contracts",
            "body": f"Could not merge stats and contracts: {e}"
        }





event = {
    "player_stats_bucket_name": "nhlapi-data",
    "player_stats_prefix": "players/player_stats/player_stats.csv",
    "player_contracts_bucket_name": "puckpedia",
    "player_current_contracts_prefix": "players/current_contracts/current_contracts.csv",
    "player_historical_contracts_prefix": "players/historical_contracts/historical_contracts.csv",
    "merged_data_bucket_name": "puckpedia",
    "merged_data_prefix": "players/merged_data/"
}

print(lambda_handler(event, None))