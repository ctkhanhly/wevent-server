import json
import boto3
from botocore.exceptions import ClientError
import logging
import pprint
import time
import uuid
import os


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
SECRET_KEY = os.environ['AWS_SECRET_KEY']
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

db = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
table = db.Table("Plans")

def check_start(start):
    # if not start.isdigit():
    if not isinstance(start, int):
        return "Start must be a unix timestamp"
    return None

def check_trigger_option(trigger_option):
    # either a unix timestamp after current time
    # or manual
    if trigger_option.lower() == "manual":
        return ""
    if not trigger_option.isdigit():
        return "Invalid trigger timestamp. Trigger must be either a unix time or \"manual\""
    LATENCY = 5 # in seconds
    if int(trigger_option) >= (time.time() - LATENCY):
        return None
    return "Trigger Time should be a future time"

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

'''
votes: [{ event_id: S, users: [] }]
'''

def dispatch(event):
    name = event['body']['name']
    start = event['body']['start']
    trigger_option = event['body']['trigger_option'].lower()
    start_error = check_start(start)
    trigger_option_error = check_trigger_option(trigger_option)

    if start_error:
        return get_error(start_error)
    if trigger_option_error:
        return get_error(trigger_option_error)
    
    plan_id = uuid.uuid4().hex

    try:
        response = table.put_item(
        Item={
                'plan_id': plan_id,
                'name': name,
                'start': start,
                'trigger_option': trigger_option,
                'invitees': [],
                'votes': [],
                'selected_event': ""
            }
        )
    except Exception as e:
        raise IOError(e)
    body = {
        'plan_id': plan_id
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


def lambda_handler(event, context):
    logger.debug('event={}\ncontext={}'.format(event, context))
    response = dispatch(event)
    logger.debug(response)
    return response

