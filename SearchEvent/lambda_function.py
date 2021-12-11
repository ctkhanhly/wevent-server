import json
import boto3
from botocore.exceptions import ClientError
import logging
import pprint
from boto3.dynamodb.conditions import Key
from datetime import datetime
import time
import os
import functools
import ast
import decimal


# https://www.fernandomc.com/posts/ten-examples-of-getting-data-from-dynamodb-with-python-and-boto3/

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
MAX_NUMBER_OF_EVENTS = 20

NEIGHBORHOODS = []
CATEGORIES = []

ACCESS_KEY = os.environ['AWS_ACCESS_KEY_']
SECRET_KEY = os.environ['AWS_SECRET_KEY_']
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

db = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )

db = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
events_table = db.Table("Events")
venues_table = db.Table("Venues")


def check_neighborhood(neighborhood):
    # if neighborhood not in NEIGHBORHOODS:
    #     return "Invalid neighborhood"
    return None

def check_start(start):
    # print('check start', isinstance(start, int), start.isdigit())
    if not isinstance(start, int) and not start.isdigit():
        # if not isinstance(start, int):
        return "Start must be a unix timestamp"
    return None

def check_category(category):
    # if category not in CATEGORIES:
    #     return "Invalid category"
    return None

def get_error(message):
    print('get_error message', message)
    body = {
            'code' : 500,
            'message': message
        }
    return {
        'statusCode': 500,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(body)
    }

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def event_to_event_response(event, neighborhood):
    event = ast.literal_eval((json.dumps(event, cls=DecimalEncoder)))
    start = event['start']
    end = event['end']
    event['start'] = datetime.utcfromtimestamp(start).strftime(DATETIME_FORMAT)
    event['end'] = datetime.utcfromtimestamp(end).strftime(DATETIME_FORMAT)
    venue_id = event['venue_id']
    if isinstance(venue_id, str):
        venue_id = int(venue_id)
        print('venue_id', venue_id)
    
    try:
        response = venues_table.get_item(Key={'venue_id': venue_id, 'neighborhood': neighborhood })
        print('got response from venues', response)
        if 'Item' not in response:
            return None
        venue = response['Item']
        event['venue_name'] = venue['venue_name']
        event['full_address'] = venue['full_address'] # Double check schema
        return event
    except ClientError as e:
        print(e.response['Error']['Message'])
        return None
    

def remove_neighborhood(event):
    if 'neighborhood' in event:
        del event['neighborhood']
    return event

def change_start(event):
    start = event['start']
    end = event['end']
    if isinstance(start, int):
        event['start'] = start
        event['end'] = end
        return event
    try:
        # _ = datetime.strptime(start, DATETIME_FORMAT)
        start = time.mktime(datetime.strptime(start, DATETIME_FORMAT).timetuple())
        end = time.mktime(datetime.strptime(end, DATETIME_FORMAT).timetuple())
        event['start'] = start
        event['end'] = end
        return event
    except ValueError:
        start = int(start)
        end = int(end)
        event['start'] = start
        event['end'] = end
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
    
    if isinstance(start, str):
        start = int(start)

    try:
        response = events_table.query(
            IndexName='category-index',
            KeyConditionExpression=Key('category').eq(category)
            # KeyConditionExpression=Key('start').gte(start) & Key('category').eq(category)
        )
        print('response from events table', response)
        events = response.get('Items', [])[:MAX_NUMBER_OF_EVENTS]
        events = list(map(change_start, events))
        print('finished change_start')
        events = list(filter(lambda event: event['start'] >= start, events))
        print('finished filter start')
        events = map(functools.partial(event_to_event_response, neighborhood=neighborhood), events)
        # events = list(map(event_to_event_response, events))
        print('finished event_to_event_response')
        events = list(filter(lambda event: event is not None, events))
        # events = list(map(remove_neighborhood, events))

        print('events', events)
        body = {
            'results' : events
        }
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps(body)
        }
    except ClientError as e:
        # return get_error(e)
        return get_error(e.response['Error']['Message'])
    except Exception as e:
        # raise IOError(e)
        return get_error(e)
        

def lambda_handler(event, context):
    logger.debug('event={}\ncontext={}'.format(event, context))
    response = dispatch(event)
    logger.debug(response)
    return response

