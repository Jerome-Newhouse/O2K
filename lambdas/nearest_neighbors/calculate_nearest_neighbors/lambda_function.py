import boto3
import pandas as pd
from io import StringIO
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
import pickle
import io
import joblib
import time



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

def calculate_nearest_neighbors(data):
    try:
        features = ['goals_per_game_y', 'assists_per_game_y', 'points_per_game_y', 'even_strength_points_per_game_y', 'power_play_points_per_game_y', 'goals_per_60_y', 'assists_per_60_y', 'points_per_60_y', 'timeOnIcePerGame_y', 'shotsBlockedByPlayer_y', 'onIce_corsiPercentage_y', 'onIce_xGoalsPercentage_y']
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
        # features = ['goals_y', 'assists_y', 'plusMinus_y', 'points_y', 'pointsPerGame_y', 'timeOnIcePerGame_y', 'shotsBlockedByPlayer_y', 'onIce_corsiPercentage_y']
        features = ['goals_per_game_y', 'assists_per_game_y', 'points_per_game_y', 'even_strength_points_per_game_y', 'power_play_points_per_game_y', 'goals_per_60_y', 'assists_per_60_y', 'points_per_60_y', 'timeOnIcePerGame_y', 'shotsBlockedByPlayer_y', 'onIce_corsiPercentage_y', 'onIce_xGoalsPercentage_y']
        contract_data = data[data['contract_id'] == contract_id][features].fillna(0)
        contract_data_scaled = scaler.transform(contract_data)
        distances, indices = nbrs.kneighbors(contract_data_scaled)
        
       
        similar_contracts = data.iloc[indices[0]]
        
        return {
            "statusCode": 200,
            "message": "Similar contracts found successfully",
            "body": similar_contracts
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Similar contracts not found",
            "body": f"Similar contracts not found: {e}"
        }

def get_next_season(season):
    start = season // 10000   
    end = season % 10000      
    return (start + 1) * 10000 + (end + 1)

def get_next_contract_info(contract_id, player_id, data):
    try:
        
        player_contracts = data[data['playerId'] == player_id]
        last_season_current_contract = player_contracts[player_contracts['contract_id'] == contract_id]['season'].max() 
        next_season = get_next_season(last_season_current_contract)
        next_contract_info = player_contracts[player_contracts['season'] == next_season]
        next_contract_id = next_contract_info['contract_id'].iloc[0]
        next_contract_info = player_contracts[player_contracts['contract_id'] == next_contract_id]
        
        
    
        if next_contract_info.empty:
            return {
                "statusCode": 404,
                "message": "Next contract info not found",
                "body": f"Next contract info not found: {next_contract_info}"
            }
        
        return {
            "statusCode": 200,
            "message": "Contract info found successfully",
            "body": next_contract_info
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Contract info not found",
            "body": f"Contract info not found: {e}"
        }

    

def lambda_handler(event, context):
    try:
        data = get_data(event['bucket_name'], event['average_stats_prefix'])
        if data['statusCode'] == 200:
            nearest_neighbors = calculate_nearest_neighbors(data['body'])
            if nearest_neighbors['statusCode'] == 200:
                similar_contracts = find_similar_contracts(event['contract_id'], data['body'], nearest_neighbors['body'])
                if similar_contracts['statusCode'] == 200:
                    all_contracts = get_data(event['contract_bucket_name'], event['contract_prefix'])
                    
                    if all_contracts['statusCode'] == 200:
                        next_contracts = []
                        for index, row in similar_contracts['body'].iterrows():
                            if row['contract_id'] != event['contract_id']:
                                next_contract_info = get_next_contract_info(row['contract_id'], row['playerId'], all_contracts['body'])
                                if next_contract_info['statusCode'] == 200:
                                    next_contracts.append(next_contract_info['body'])
                                else:
                                    current_contract = all_contracts['body'][all_contracts['body']['contract_id'] == row['contract_id']]
                                    next_contracts.append(current_contract)
                     
                        next_contracts = pd.concat(next_contracts)
                        return {
                            "statusCode": 200,
                            "message": "Next contracts found successfully",
                            "body": next_contracts
                        }
                    else:
                        return {
                            "statusCode": 404,
                            "message": "All contracts not found",
                            "body": f"All contracts not found: {all_contracts['body']}"
                        }
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
                "message": "Data not found",
                "body": f"Data not found: {data['body']}"
            }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Nearest neighbors not calculated",
            "body": f"Nearest neighbors not calculated: {e}"
        }
        
        
        

event = {
    "bucket_name": "contract-stats-merged-data",
    "average_stats_prefix": "players/average_stats/average_stats_advanced_contracts.csv",
    "nearest_neighbors_prefix": "players/nearest_neighbors/nearest_neighbors_advanced_contracts.pkl",
    "contract_id": 6131,
    "contract_bucket_name": "puckpedia",
    "contract_prefix": "players/merged_data/merged_data.csv"
}

lambda_result = lambda_handler(event, {})
# print(lambda_result)
# print(lambda_result['body'][['contract_id', 'lastName', 'value', 'length', 'season', 'percentage_of_season_salary_cap', 'cap_hit', 'aav']])
# print(lambda_handler(event, {}))

player_contracts = lambda_result['body']
player_contracts['average_percentage_of_season_salary_cap'] = player_contracts.groupby('contract_id')['percentage_of_season_salary_cap'].transform('mean')

player_contracts['season_span'] = player_contracts.groupby('contract_id')['season'].transform(lambda x: f"{x.min()} - {x.max()}")

first_entry_per_contract = player_contracts.groupby('contract_id').first()


print(first_entry_per_contract[[ 'lastName', 'value', 'length', 'cap_hit', 'aav', 'average_percentage_of_season_salary_cap', 'season_span']])







