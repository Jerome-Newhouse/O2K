import boto3
import pandas as pd
from io import StringIO






def get_data(bucket_name, prefix, year):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        prefix = prefix + str(year) + "/" + f"skaters_{year}.csv"
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        data = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(data))
        data['season'] = (data['season'].astype(str) + (data['season'] + 1).astype(str)).astype(int)
        
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
        merged_data_list = []
        for year in event['years']:
            data = get_data(event['bucket_name'], event['data_prefix'], year)
            if data['statusCode'] == 200:
                merged_data_list.append(data['body'])
        merged_data = pd.concat(merged_data_list)
        save_to_s3_response = save_to_s3(merged_data, event['merged_data_bucket_name'], event['merged_data_prefix'])
        if save_to_s3_response['statusCode'] == 200:
            return {
                "statusCode": 200,
                "message": "Merge all years data",
                "body": "Merge all years data"
            }
        else:
            return {
                "statusCode": 404,
                "message": "Data not saved",
                "body": f"Data not saved: {save_to_s3_response['body']}"
            }   
    except Exception as e:
        return {
            "statusCode": 500,
            "message": "Error merging all years data",
            "body": f"Error merging all years data: {e}"
        }
        
        
        
        
event = {
    "bucket_name": "money-puck-data",
    "years": [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    "data_prefix": "skaters/",
    "merged_data_prefix": "merged_data/skaters/merged_data.csv",
    "merged_data_bucket_name": "money-puck-data"
}

print(lambda_handler(event, None))




