import json
import boto3
from botocore.exceptions import ClientError
import logging
import pprint
from boto3.dynamodb.conditions import Key
from datetime import datetime
import os


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

NEIGHBORHOODS = []
CATEGORIES = []

ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
SECRET_KEY = os.environ['AWS_SECRET_KEY']

db = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
table = db.Table("Events")


def check_neighborhood(neighborhood):
    if neighborhood not in NEIGHBORHOODS:
        return "Invalid neighborhood"
    return ""

def check_start(start):
    # if not start.isdigit():
    if not isinstance(start, int):
        return "Start must be a unix timestamp"
    return ""

def check_category(category):
    if category not in CATEGORIES:
        return "Invalid category"
    return ""

def get_error(message):
    return {
        'statusCode': 500,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': {
            'code' : 500,
            'message': e
        }
    }

def event_to_event_response(event):
    start = event['start']
    end = event['end']
    event['start'] = datetime.utcfromtimestamp(start).strftime('%Y-%m-%dT%H:%M:%SZ')
    event['end'] = datetime.utcfromtimestamp(end).strftime('%Y-%m-%dT%H:%M:%SZ')
    return event

def dispatch(event):
    
    neighborhood = event['queryStringParameters']['neighborhood']
    start = event['queryStringParameters']['start'] # unix timestamp
    category = event['queryStringParameters']['category']

    neighborhood_error = check_neighborhood(neighborhood)
    start_error = check_start(start)
    category_error = check_category(category)

    if neighborhood_error:
        return get_error(neighborhood_error)
    if start_error:
        return get_error(start_error)
    if category_error:
        return get_error(category_error)

    try:
        response = table.query(
            KeyConditionExpression=Key('start').ge(start) 
            & KeyConditionExpression=Key('neighborhood').eq(neighborhood)
            & KeyConditionExpression=Key('category').eq(category)
        )
        events = response.get('Items', [])
        events = list(map(event_to_event_response, events))
        body = {
            'results' : events
        }
        return return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            }
            'body': json.dumps(body)
        }
    except Exception as e:
        # raise IOError(e)
        return get_error(e)
        

def lambda_handler(event, context):
    logger.debug('event={}\ncontext={}'.format(event, context))
    response = dispatch(event)
    logger.debug(response)
    return response

