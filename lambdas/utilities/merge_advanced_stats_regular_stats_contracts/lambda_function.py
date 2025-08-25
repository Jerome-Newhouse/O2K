import boto3
import pandas as pd
from io import StringIO
import os


# def get_large_data(bucket_name, prefix):
#     try:
#         s3 = boto3.client("s3", region_name="us-east-2")
#         tmp_path = f"/tmp/{os.path.basename(prefix)}"

#         s3.download_file(bucket_name, prefix, tmp_path)

#         data = pd.read_csv(tmp_path)
#         return {
#             "statusCode": 200,
#             "message": "Data retrieved successfully",
#             "body": data
#         }
#     except Exception as e:
#         return {
#             "statusCode": 404,
#             "message": "Data not found",
#             "body": f"Data not found: {e}"
#         }

def get_data(bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        data = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(data))
        return {
            "statusCode": 200,
            "message": "Data retrieved successfully",
            "body": data
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Data not found",
            "body": f"Data not found: {e}"
        }   


def merge_data(contract_stats, advanced_stats):
    merged_stats = pd.merge(contract_stats, advanced_stats, on='playerId', how='left')
    return merged_stats


def save_to_s3(data, bucket_name, prefix):

    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.put_object(Bucket=bucket_name, Key=prefix, Body=data.to_csv(index=False))
        return {
            "statusCode": 200,
            "message": "Data saved successfully",
            "body": "Data saved successfully"
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Data not saved",
            "body": f"Data not saved: {e}"
        }

def lambda_handler(event, context):
    try:
        contract_stats = get_data(event['bucket_name'], event['contract_stats_prefix'])
        if contract_stats['statusCode'] == 200:
            advanced_stats = get_data(event['advanced_stats_bucket_name'], event['advanced_stats_prefix'])
            
            if advanced_stats['statusCode'] == 200:
                contract_stats['body'].drop(columns=['position', 'season'], inplace=True)
                merged_stats = pd.merge(contract_stats['body'], advanced_stats['body'], left_on=['playerId', 'seasonId'], right_on=['playerId', 'season'], how='inner', suffixes=("", ""))
                
                save_to_s3_response = save_to_s3(merged_stats, event['save_to_s3_bucket_name'], event['save_to_s3_prefix'])
                if save_to_s3_response['statusCode'] == 200:
                    return {
                        "statusCode": 200,
                        "message": "Merge advanced stats and regular stats",
                        "body": "Merge advanced stats and regular stats"
                    }
                else:
                    return {    
                        "statusCode": 404,
                        "message": "Data not saved",
                        "body": f"Data not saved: {save_to_s3_response['body']}"
                    }
            else:
                return {
                    "statusCode": 404,
                    "message": "Advanced stats not found",
                    "body": f"Advanced stats not found: {advanced_stats['body']}"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Contract stats not found",
                "body": f"Contract stats not found: {contract_stats['body']}"
            }
    
        
    except Exception as e:
        return {
            "statusCode": 500,
            "message": "Error merging advanced stats and regular stats",
            "body": f"Error merging advanced stats and regular stats: {e}"
        }
        
        
        
        
event = {
    "bucket_name": "puckpedia",
    "contract_stats_prefix": "players/advanced_stats/advanced_stats.csv",
    "advanced_stats_prefix": "merged_data/skaters/merged_data.csv",
    "advanced_stats_bucket_name": "money-puck-data",
    "save_to_s3_prefix": "players/merged_data/merged_data_advanced_contracts.csv",
    "save_to_s3_bucket_name": "contract-stats-merged-data"
}

print(lambda_handler(event, None))