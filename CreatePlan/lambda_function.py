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

ACCESS_KEY = os.environ['AWS_ACCESS_KEY_']
SECRET_KEY = os.environ['AWS_SECRET_KEY_']
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

db = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
table = db.Table("Plans")
user_table = db.Table("Users")

def check_start(start):
    # print('check start', isinstance(start, int), start.isdigit())
    if not isinstance(start, int) and not start.isdigit():
        # if not isinstance(start, int):
        return "Start must be a unix timestamp"
    return None

def check_trigger_option(trigger_option):
    # either a unix timestamp after current time
    # or manual
    if trigger_option.lower() == "manual":
        return None
    if not trigger_option.isdigit():
        return "Invalid trigger timestamp. Trigger must be either a unix time or \"manual\""
    LATENCY = 5 # in seconds
    if int(trigger_option) >= (time.time() - LATENCY):
        return None
    return "Trigger Time should be a future time"

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

'''
votes: [{ event_id: S, users: [] }]
'''

def add_plan_to_user_table(plan_id, user_id):
    user_response = user_table.get_item(Key={'email': user_id}, ConsistentRead=True)
    if 'Item' not in user_response:
        return get_error(f"No user exists with email: {user_id}")
    plans = user_response['Item']['plan_ids']
    def filter_plan(plan):
        return plan['plan_id'] == plan_id
    plan_existed = list(filter(filter_plan, plans))
    if len(plan_existed) == 0:
        plans.append({'plan_id': plan_id, 'isHost': True})
    user_update_response = user_table.update_item(
            Key={
                'email': user_id
            },
            UpdateExpression="set plan_ids=:p",
            ExpressionAttributeValues={
                ':p': plans
            },
            ReturnValues="UPDATED_NEW"
        )

def dispatch(event):
    body = json.loads(event['body'])
    name = body['name']
    start = body['start']
    trigger_option = body['trigger_option'].lower()
    host_id = body['host_id']
    
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
                'selected_event': "",
                'host_id': host_id
            }
        )

        add_plan_to_user_table(plan_id, host_id)
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