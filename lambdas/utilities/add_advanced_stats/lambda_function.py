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


def get_merged_stats(bucket_name, prefix):
    try:
        s3 = boto3.client("s3", region_name="us-east-2")
        response = s3.get_object(Bucket=bucket_name, Key=prefix)
        data = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(data))
        return {
            "statusCode": 200,
            "message": "Merged stats retrieved successfully",
            "body": data
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not retrieve merged stats",
            "body": f"Could not retrieve merged stats: {e}"
        }

def calculate_advanced_stats(merged_stats):
    try:
        merged_stats["goals_per_game"] = merged_stats["goals"] / merged_stats["gamesPlayed"]
        merged_stats["assists_per_game"] = merged_stats["assists"] / merged_stats["gamesPlayed"]
        merged_stats["points_per_game"] = merged_stats["points"] / merged_stats["gamesPlayed"]
        merged_stats["shots_per_game"] = merged_stats["shots"] / merged_stats["gamesPlayed"]
        merged_stats['faceoffWinPct'] = merged_stats['faceoffWinPct'].fillna(0)
        merged_stats['even_strength_goals_per_game'] = merged_stats['evGoals'] / merged_stats['gamesPlayed']
        merged_stats['even_strength_points_per_game'] = merged_stats['evPoints'] / merged_stats['gamesPlayed']
        merged_stats['power_play_goals_per_game'] = merged_stats['ppGoals'] / merged_stats['gamesPlayed']
        merged_stats['power_play_points_per_game'] = merged_stats['ppPoints'] / merged_stats['gamesPlayed']
    
        merged_stats['power_play_point_percentage'] = merged_stats['ppPoints'] / merged_stats['points']
        merged_stats['even_strength_point_percentage'] = merged_stats['evPoints'] / merged_stats['points']
        merged_stats['even_strength_goal_percentage'] = merged_stats['evGoals'] / merged_stats['goals']
        merged_stats['power_play_goal_percentage'] = merged_stats['ppGoals'] / merged_stats['goals']
        merged_stats['short_handed_goal_percentage'] = merged_stats['shGoals'] / merged_stats['goals']
        merged_stats['short_handed_point_percentage'] = merged_stats['shPoints'] / merged_stats['points']
        
        merged_stats['goals_per_60'] = (merged_stats['goals'] / (merged_stats['timeOnIcePerGame'] * 60)) * 60
        merged_stats['assists_per_60'] = (merged_stats['assists'] / (merged_stats['timeOnIcePerGame'] * 60)) * 60
        merged_stats['points_per_60'] = (merged_stats['points'] / (merged_stats['timeOnIcePerGame'] * 60)) * 60
        merged_stats['shots_per_60'] = (merged_stats['shots'] / (merged_stats['timeOnIcePerGame'] * 60)) * 60
      
        
        
        
        return {
            "statusCode": 200,
            "message": "Advanced stats calculated successfully",
            "body": merged_stats
        }
    except Exception as e:
        return {
            "statusCode": 404,
            "message": "Could not calculate advanced stats",
            "body": f"Could not calculate advanced stats: {e}"
        }

def lambda_handler(event, context):
    try:
        merged_stats = get_merged_stats(event['bucket_name'], event['merged_stats_prefix'])
        print(merged_stats)
        advanced_stats = calculate_advanced_stats(merged_stats['body'])
        print(advanced_stats)
        # if merged_stats['statusCode'] == 200:
        #     advanced_stats = calculate_advanced_stats(merged_stats['body'])
        #     if advanced_stats['statusCode'] == 200:
        #         merged_stats = merged_stats['body']
        #         advanced_stats = advanced_stats['body']
        #         merged_stats = pd.merge(merged_stats, advanced_stats, on='playerId', how='left')
        #         save_to_s3(merged_stats, event['bucket_name'], event['merged_stats_prefix'])
        return {
            "statusCode": 200,
            "message": "Add advanced stats",
            "body": "Add advanced stats"
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "message": "Error adding advanced stats",
            "body": f"Error adding advanced stats: {e}"
        }
        
        
        
        
event = {
    "bucket_name": "puckpedia",
    "merged_stats_prefix": "players/merged_data/merged_data.csv",
    "advanced_stats_prefix": "players/advanced_stats/advanced_stats.csv",
    "advanced_stats_bucket_name": "puckpedia",
}

print(lambda_handler(event, None))
    
        
        