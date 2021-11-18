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
    return ""

def check_trigger_option(trigger_option):
    # either a unix timestamp after current time
    # or manual
    if trigger_option.lower() == "manual":
        return ""
    if not trigger_option.isdigit():
        return "Invalid trigger timestamp"
    LATENCY = 5 # in seconds
    return int(trigger_option) >= (time.time() - LATENCY)

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
    votes = response['Item']['votes']
    def map_event(e):
        if e.event_id == event_id:
            if user_id not in e.users:
                e.users.append(user_id)
        return e
    votes = list(map(map_event, votes))
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
    except Exception as e:
        # raise IOError(e)
        return get_error(e)

def process_add_friend_update(body):
    plan_id = body['plan_id']
    event_id = body['event_id']

    response = table.get_item(Key={'plan_id': plan_id}, ConsistentRead=True)
    invitees = response['Item']['invitees']
    if user_id not in invitees:
        invitees.append(user_id)
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
    except Exception as e:
        # raise IOError(e)
        return get_error(e)

def process_manual_trigger_update(body):
    plan_id = body['plan_id']
    event_id = body['event_id']

    response = table.get_item(Key={'plan_id': plan_id}, ConsistentRead=True)
    votes = response['Item']['votes']
    def filter_event(e):
        return e.event_id == event_id
    has_event = list(filter(filter_event, votes))
    if len(has_event) == 0:
        return get_error("Event was not selected in plan")
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
    except Exception as e:
        # raise IOError(e)
        return get_error(e)

def process_add_event(body):
    plan_id = body['plan_id']
    event_id = body['event_id']

    response = table.get_item(Key={'plan_id': plan_id}, ConsistentRead=True)
    votes = response['Item']['votes']
    def filter_event(e):
        return e.event_id == event_id
    has_event = list(filter(filter_event, votes))
    if len(has_event) == 0:
        votes.append({'event_id': event_id, 'users': []})
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
    except Exception as e:
        # raise IOError(e)
        return get_error(e)

def dispatch(event):

    update_type = event['body']['update_type']
    if update_type == "vote":
        return process_vote_update(event['body'])
    elif update_type == "add_friend":
        return process_add_friend_update(event['body'])
    elif update_type == "manual_trigger":
        return process_add_friend_update(event['body'])
    elif update_type == "add_event":
        return process_add_event(event['body'])
    else:
        return get_error("Unknown update type")


def lambda_handler(event, context):
    logger.debug('event={}\ncontext={}'.format(event, context))
    response = dispatch(event)
    logger.debug(response)
    return response