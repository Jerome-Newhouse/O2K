import boto3
import pandas as pd
from io import StringIO

    
    
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

def calculate_average_stats(data):
    try:
        stats = ['goals_per_game', 'assists_per_game', 'points_per_game', 'even_strength_points_per_game', 'power_play_points_per_game', 'goals_per_60', 'assists_per_60', 'points_per_60', 'timeOnIcePerGame', 'shotsBlockedByPlayer', 'onIce_corsiPercentage', 'onIce_xGoalsPercentage' ]
        df_overall = data[data["situation"] == "all"].copy()
        df_overall = df_overall.sort_values(["contract_id", "seasonId"], ascending=[True, True])
        weighted_stats = []
        
        for contract_id, group in df_overall.groupby("contract_id"):
            group = group.sort_values("seasonId", ascending=False)  # recent first
            n = len(group)
            
            # Assign weights
            if n == 1:
                weights = [1.0]
            elif n == 2:
                weights = [0.7, 0.3]
            else:
                remaining = 0.10 / (n - 2)
                weights = [0.7, 0.2] + [remaining] * (n - 2)
            
            group["weight"] = weights
            
            # Compute weighted average for this contract
            weighted_avg = (group[stats].multiply(group["weight"], axis=0)).sum()
            weighted_avg["contract_id"] = contract_id
            weighted_stats.append(weighted_avg)
            
        average_stats = pd.DataFrame(weighted_stats).set_index("contract_id")
      
        # average_stats = df_overall.groupby('contract_id')[stats].mean()
        return {
            "statusCode": 200,
            "message": "Average stats calculated successfully",
            "body": average_stats
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Average stats not calculated",
            "body": f"Average stats not calculated: {e}"
        }


def merge_data(data, average_stats):
    try:
        contract_info = data.drop_duplicates("contract_id")
        # print(contract_info[['contract_id']])
        merged_data = pd.merge(contract_info, average_stats, on='contract_id', how='left')
        print(merged_data)
        
        return {
            "statusCode": 200,
            "message": "Data merged successfully",
            "body": merged_data
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Data not merged",
            "body": f"Data not merged: {e}"
        }

def save_data(bucket_name, prefix, data):
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
        data = get_data(event['bucket_name'], event['merged_data_prefix'])
        if data['statusCode'] == 200:
            average_stats = calculate_average_stats(data['body'])
            # print(average_stats['body'])
            if average_stats['statusCode'] == 200:
                merged_data = merge_data(data['body'], average_stats['body'])
                if merged_data['statusCode'] == 200:
                    save_data_result = save_data(event['bucket_name'], event['average_stats_prefix'], merged_data['body'])
                    if save_data_result['statusCode'] == 200:
                        return {
                            "statusCode": 200,
                            "message": "Data merged successfully",
                            "body": "Data merged successfully"
                        }
                    else:
                        return {
                            "statusCode": 404,
                            "message": "Data not saved", 
                            "body": f"Data not saved: {save_data_result ['body']}"
                        }
                    
                else:
                    return {    
                        "statusCode": 404,
                        "message": "Data not merged",
                        "body": f"Data not merged: {merged_data['body']}"
                    }
            else:
                return {
                    "statusCode": 404,
                    "message": "Average stats not calculated",
                    "body": f"Average stats not calculated: {average_stats['body']}"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Data not found",
                "body": f"Data not found: {data['body']}"
            }
        
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Average stats not calculated",
            "body": f"Average stats not calculated: {e}"
        }
        
        
        
        
event = {
    "bucket_name": "contract-stats-merged-data",
    "merged_data_prefix": "players/merged_data/merged_data_advanced_contracts.csv",
    "average_stats_prefix": "players/average_stats/average_stats_advanced_contracts.csv"
}

print(lambda_handler(event, {}))



        