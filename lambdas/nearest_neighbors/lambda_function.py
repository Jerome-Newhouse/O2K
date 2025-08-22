import boto3
import pandas as pd
from io import StringIO
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()


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
        stats = ['goals', 'assists', 'plusMinus', 'points', 'pointsPerGame', 'timeOnIcePerGame', 'shotsBlockedByPlayer', 'onIce_corsiPercentage']
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
        print(average_stats)
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
        
        
def calculate_nearest_neighbors(data):
    try:
        features = ['goals_y', 'assists_y', 'plusMinus_y', 'points_y', 'pointsPerGame_y', 'timeOnIcePerGame_y', 'shotsBlockedByPlayer_y', 'onIce_corsiPercentage_y']
        data = data[features].fillna(0)
        data_scaled = scaler.fit_transform(data)
        nbrs = NearestNeighbors(n_neighbors=10, algorithm='ball_tree').fit(data_scaled)
        return {
            "statusCode": 200,
            "message": "Nearest neighbors calculated successfully",
            "body": nbrs
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Nearest neighbors not calculated",
            "body": f"Nearest neighbors not calculated: {e}"
        }

def find_similar_contracts(contract_id, data, nbrs):
    try:
        features = ['goals_y', 'assists_y', 'plusMinus_y', 'points_y', 'pointsPerGame_y', 'timeOnIcePerGame_y', 'shotsBlockedByPlayer_y', 'onIce_corsiPercentage_y']
        contract_data = data[data['contract_id'] == contract_id][features].fillna(0)
        contract_data_scaled = scaler.transform(contract_data)
        distances, indices = nbrs.kneighbors(contract_data_scaled)
        print(distances)
        print(indices)
       
        similar_contracts = data.iloc[indices[0]]
        print(similar_contracts[['contract_id', 'lastName']])
        return {
            "statusCode": 200,
            "message": "Similar contracts found successfully",
            "body": indices
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Similar contracts not found",
            "body": f"Similar contracts not found: {e}"
        }


        
        
def find_next_contract(player_id, contract_id, data):
    try:
        player_data = data[data['playerId'] == player_id]
        player_data = player_data.sort_values("seasonId", ascending=False)
        last_season = player_data[player_data['contract_id'] == contract_id]['seasonId'].values[0]
        print(player_data)  
        print(last_season)
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Next contract not found",
            "body": f"Next contract not found: {e}"
        }

def lambda_handler(event, context):
    try:
        merged_data = get_data(event['bucket_name'], event['merged_data_prefix'])
        if merged_data['statusCode'] == 200:
            average_stats = calculate_average_stats(merged_data['body'])
            if average_stats['statusCode'] == 200:
                merged_data_with_average_stats = merge_data(merged_data['body'], average_stats['body'])
                if merged_data_with_average_stats['statusCode'] == 200:
                    nearest_neighbors = calculate_nearest_neighbors(merged_data_with_average_stats['body'])
                    if nearest_neighbors['statusCode'] == 200:
                        print(nearest_neighbors['body'])
                        similar_contracts = find_similar_contracts(event['contract_id'], merged_data_with_average_stats['body'], nearest_neighbors['body'])
                        print(similar_contracts['body'])
                        if similar_contracts['statusCode'] == 200:
                            print(similar_contracts['body'])
                        else:
                            return {
                                "statusCode": 404,
                                "message": "Similar contracts not found",
                                "body": f"Similar contracts not found: {similar_contracts['body']}"
                            }
                    else:
                        return {
                            "statusCode": 404,
                            "message": "Nearest neighbors not calculated",
                            "body": f"Nearest neighbors not calculated: {nearest_neighbors['body']}"
                        }
                else:
                    return {
                        "statusCode": 404,
                        "message": "Data not merged",
                        "body": f"Data not merged: {merged_data_with_average_stats['body']}"
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
                "message": "Nearest neighbors",
                "body": "Nearest neighbors"
            }
    except Exception as e:
        return {
            "statusCode": 500,
            "message": "Error nearest neighbors",
            "body": f"Error nearest neighbors: {e}"
        }
        
        
        
        
event = {
    "bucket_name": "contract-stats-merged-data",
    "merged_data_prefix": "players/merged_data/merged_data_advanced_contracts.csv",
    "contract_id": 9
}


print(lambda_handler(event, None))
