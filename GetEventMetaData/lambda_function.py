import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import logging
import pprint
import time
import os
from datetime import datetime


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ACCESS_KEY = os.environ['AWS_ACCESS_KEY_']
SECRET_KEY = os.environ['AWS_SECRET_KEY_']

db = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
venues_table = db.Table("Venues")
events_table = db.Table("Events")

def get_neighborhoods():
    try:
        response = venues_table.get_item(Key={'venue_id': 0, 'neighborhood': 'All'})
        if 'Item' not in response:
            return None
        neighborhoods = response['Item']['neighborhoods']        
                    
        hood = set()
        for n in neighborhoods:
            hood.add(n)
        return(sorted(hood))
            
    except ClientError as e:
        # return get_error(e)
        return None
    except Exception as e:
        # raise IOError(e)
        return None

def get_categories():
    try:
        response = events_table.get_item(Key={'event_id': 0, 'category': 'All'})
        if 'Item' not in response:
            return None
        categories = response['Item']['categories']        
                    
        cat = set()
        for c in categories:
            cat.add(c)
        return(sorted(cat))
        
    except ClientError as e:
        # return get_error(e)
        return None
    except Exception as e:
        # raise IOError(e)
        return None

def dispatch():
    try:
        neighborhoods = get_neighborhoods()
        categories = get_categories()
        body = {
            'neighborhoods': neighborhoods,
            'categories': categories
        }
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body' : json.dumps(body)
        }
    except:
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body' : json.dumps("Error")
        }
   

def lambda_handler(event, context):
    logger.debug('event={}\ncontext={}'.format(event, context))
    response = dispatch()
    logger.debug(response)
    return response

