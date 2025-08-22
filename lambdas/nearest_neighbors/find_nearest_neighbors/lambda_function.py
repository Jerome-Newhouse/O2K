import boto3
import pandas as pd
from io import StringIO
import pickle
import joblib
import io
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()


def get_pickle_from_s3(bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        body = response['Body'].read()
    
        # Load back into sklearn object
        buffer = io.BytesIO(body)
        model = joblib.load(buffer)
        
        return {
            "statusCode": 200,
            "message": "Pickle retrieved successfully", 
            "body": model
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Pickle not found",
            "body": f"Pickle not found: {e}"
        }
        
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

def lambda_handler(event, context):
    try:
        nearest_neighbors = get_pickle_from_s3(event['bucket_name'], event['nearest_neighbors_prefix'])
        if nearest_neighbors['statusCode'] == 200:
            data = get_data(event['bucket_name'], event['average_stats_prefix'])
            if data['statusCode'] == 200:
                nearest_neighbors_result = find_similar_contracts(event['contract_id'], data['body'], nearest_neighbors['body'])
                print(nearest_neighbors_result)
                return {
                    "statusCode": 200,
                    "message": "Nearest neighbors found successfully",
                    "body": nearest_neighbors_result
                }
            else:
                return {
                    "statusCode": 404,
                    "message": "Data not found",
                    "body": f"Data not found: {data['body']}"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Nearest neighbors not found",
                "body": f"Nearest neighbors not found: {nearest_neighbors['body']}"
            }
        
        
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Nearest neighbors not found",
            "body": f"Nearest neighbors not found: {e}"
        }
        
        
event = {
    "bucket_name": "contract-stats-merged-data",
    "nearest_neighbors_prefix": "players/nearest_neighbors/nearest_neighbors_advanced_contracts.pkl",
    "average_stats_prefix": "players/average_stats/average_stats_advanced_contracts.csv",
    "contract_id": "1234567890"
}


print(lambda_handler(event, {}))

