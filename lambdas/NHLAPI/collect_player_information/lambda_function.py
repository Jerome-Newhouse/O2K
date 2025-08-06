import requests
import boto3
import json
import logging


def get_player_information(player_id):
    try:
        logging.info(f"Getting player information for player {player_id}")
        url = f"https://api-web.nhle.com/v1/player/{player_id}/landing"
        response = requests.get(url)
        logging.info(f"Player information retrieved successfully for player {player_id}")
        return {
            "statusCode": 200,
            "message": "Player information retrieved successfully",
            "body": response.json()
        }
    except Exception as e:
        logging.error(f"Could not get player information for player {player_id}: {e}")
        return {
            "statusCode": 404,
            "message": "Could not get player information",
            "body": f"Could not get player information: {e}"
        }

def save_to_s3(data, bucket_name, prefix, player_id):
    try:
        logging.info(f"Saving player information to S3 for player {data}")
        s3 = boto3.client("s3", region_name="us-east-2")
        path = f"{prefix}{player_id}/player_information.json"
        s3.put_object(Bucket=bucket_name, Key=path, Body=json.dumps(data), ContentType='application/json')
        logging.info(f"Player information saved to S3 for player {player_id}")
        return {
            "statusCode": 200,
            "message": "Player information saved to S3",
        }
    except Exception as e:
        logging.error(f"Could not save player information to S3 for player {data['player_id']}: {e}")
        return {
            "statusCode": 404,
            "message": "Could not save player information to S3",
            "body": f"Could not save player information to S3: {e}"
        }

def clean_player_information(player_information):
    try:
        logging.info(f"Cleaning player information for player {player_information['playerId']}")
        
        player_information.pop('last5Games', None)
        player_information.pop('featuredStats', None)
        player_information.pop('careerTotals', None)
        player_information.pop('shopLink', None)
        player_information.pop('twitterLink', None)
        player_information.pop('watchLink', None)
        logging.info(f"Player information cleaned successfully for player {player_information['playerId']}")
        return {
            "statusCode": 200,
            "message": "Player information cleaned successfully",
            "body": player_information
        }
    except Exception as e:
        logging.error(f"Could not clean player information for player {player_information['player_id']}: {e}")
        return {
            "statusCode": 404,
            "message": "Could not clean player information",
            "body": f"Could not clean player information: {e}"
        }

def lambda_handler(event, context):
    try:
     
        logging.info(f"Collecting player information for player {event['player_id']}")
        player_information = get_player_information(event['player_id'])
    
        if player_information['statusCode'] == 200:
            cleaned_player_information = clean_player_information(player_information['body'])
            if cleaned_player_information['statusCode'] == 200:
                response = save_to_s3(cleaned_player_information['body'], event['bucket_name'], event['prefix'], event['player_id'])
                if response['statusCode'] == 200:
                    return {
                        "statusCode": 200,
                        "message": "Player information collected successfully",
                        "body": "Player information collected successfully"
                    }
                else:
                    return {
                        "statusCode": 404,
                        "message": "Could not clean player information",
                        "body": "Could not clean player information"
                    }
            else:
                return {
                    "statusCode": 404,
                    "message": "Could not get player information",
                    "body": "Could not get player information"
                }
        else:
            return {
                "statusCode": 404,
                "message": "Could not get player information",
                "body": "Could not get player information"
            }
    

    except Exception as e:
        logging.error(f"Could not collect player information: {e}")
        return {
            "statusCode": 404,
            "message": "Could not collect player information",

        }












# event = {
#     "bucket_name": "nhlapi-data",
#     "prefix": "players/player_info/",
#     "player_id": 8470594
# }




# print(lambda_handler(event, None))
