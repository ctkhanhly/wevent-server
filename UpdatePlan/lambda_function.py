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

db = boto3.resource(
    'dynamodb',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
table = db.Table("Plans")



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
        return "Invalid trigger timestamp"
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

def get_success_response():
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }
    }

def process_vote_update(body):
    plan_id = body['plan_id']
    event_id = body['event_id']
    user_id = body['user_id']

    response = table.get_item(Key={'plan_id': plan_id}, ConsistentRead=True)
    if 'Item' not in response:
        return get_error(f"No plan exists with plan_id: {plan_id}")
    print('item', response['Item'])
    item = response['Item']
    votes = response['Item']['votes']
    def map_event(e):
        if e['event_id'] == event_id:
            if user_id not in e['users']:
                e['users'].append(user_id)
        return e
    votes = list(map(map_event, votes))
    item['votes'] = votes
    try:
        response = table.update_item(
            Key={
                'plan_id': plan_id
            },
            UpdateExpression="set votes=:v",
            ExpressionAttributeValues={
                ':v': votes
            },
            ReturnValues="UPDATED_NEW"
        )
        # response = table.put_item(Item=item)
        return get_success_response()
    except Exception as e:
        # raise IOError(e)
        return get_error(e)

def process_add_friend_update(body):
    plan_id = body['plan_id']
    user_id = body['user_id']

    response = table.get_item(Key={'plan_id': plan_id}, ConsistentRead=True)
    if 'Item' not in response:
        return get_error(f"No plan exists with plan_id: {plan_id}")
    print('item', response['Item'])
    item = response['Item']
    invitees = response['Item']['invitees']
    if user_id not in invitees:
        invitees.append(user_id)
    item['invitees'] = invitees
    try:
        response = table.update_item(
            Key={
                'plan_id': plan_id
            },
            UpdateExpression="set invitees=:i",
            ExpressionAttributeValues={
                ':i': invitees
            },
            ReturnValues="UPDATED_NEW"
        )
        # response = table.put_item(Item=item)
        return get_success_response()
    except Exception as e:
        # raise IOError(e)
        return get_error(e)

def process_manual_trigger_update(body):
    plan_id = body['plan_id']
    event_id = body['event_id']

    response = table.get_item(Key={'plan_id': plan_id}, ConsistentRead=True)
    if 'Item' not in response:
        return get_error(f"No plan exists with plan_id: {plan_id}")
    print('item', response['Item'])
    item = response['Item']
    votes = response['Item']['votes']
    def filter_event(e):
        return e['event_id'] == event_id
    has_event = list(filter(filter_event, votes))
    if len(has_event) == 0:
        return get_error("Event was not selected in plan")
    item['selected_event'] = event_id
    try:
        response = table.update_item(
            Key={
                'plan_id': plan_id
            },
            UpdateExpression="set selected_event=:s, votes=:v",
            ExpressionAttributeValues={
                ':s': event_id,
                ':v': votes
            },
            ReturnValues="UPDATED_NEW"
        )
        # response = table.put_item(Item=item)
        return get_success_response()
    except Exception as e:
        # raise IOError(e)
        return get_error(e)

def process_add_event(body):
    plan_id = body['plan_id']
    event_id = body['event_id']

    response = table.get_item(Key={'plan_id': plan_id}, ConsistentRead=True)
    if 'Item' not in response:
        return get_error(f"No plan exists with plan_id: {plan_id}")
    print('item', response['Item'])
    item = response['Item']
    votes = response['Item']['votes']
    def filter_event(e):
        return e['event_id'] == event_id
    has_event = list(filter(filter_event, votes))
    if len(has_event) == 0:
        votes.append({'event_id': event_id, 'users': []})
    item['votes'] = votes
    try:
        response = table.update_item(
            Key={
                'plan_id': plan_id
            },
            UpdateExpression="set votes=:v",
            ExpressionAttributeValues={
                ':v': votes
            },
            ReturnValues="UPDATED_NEW"
        )
        # response = table.put_item(Item=item)
        return get_success_response()
    except Exception as e:
        # raise IOError(e)
        return get_error(e)

def dispatch(event):
    body = json.loads(event['body'])
    update_type = body['update_type']
    if update_type == "vote":
        return process_vote_update(body)
    elif update_type == "add_friend":
        return process_add_friend_update(body)
    elif update_type == "manual_trigger":
        return process_manual_trigger_update(body)
    elif update_type == "add_event":
        return process_add_event(body)
    else:
        return get_error("Unknown update type")


def lambda_handler(event, context):
    logger.debug('event={}\ncontext={}'.format(event, context))
    response = dispatch(event)
    logger.debug(response)
    return response