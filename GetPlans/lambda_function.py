import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import logging
import pprint
import time
import os
from datetime import datetime
import decimal
import ast


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
plans_table = db.Table("Plans")
events_table = db.Table("Events")

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

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

def date_from_unix(unix):
    return datetime.utcfromtimestamp(unix).strftime(DATETIME_FORMAT)

def event_response(vote):
    try:
        response = events_table.get_item(Key={'event_id': vote['event_id']})
        if 'Item' not in response:
            return None
        item = response['Item']
        del vote['event_id']
        vote['event'] = {
            'event_id': item['event_id'],
            'event_name':  item['event_name'],
            'description': item['description'],
            'start': date_from_unix(item['start']),
            'end': date_from_unix(item['end']),
            'imageurl': item['imageurl'],
            'full_address': item['full_address']
        }
        return vote
    except ClientError as e:
        # return get_error(e)
        return None
    except Exception as e:
        # raise IOError(e)
        return None

def plan_to_plan_response(plan):
    plan = ast.literal_eval((json.dumps(plan, cls=DecimalEncoder)))
    plan['votes'] = list(map(event_response, plan['votes']))
    plan['votes'] = list(filter(lambda vote: vote != None, plan['votes']))
    return plan

def dispatch(event):
    user_id = event['queryStringParameters']['user_id']

    try:
        response = plans_table.query(
            IndexName='host-index',
            KeyConditionExpression=Key('host_id').eq(user_id)
        )
        plans = response.get('Items', [])
        plans = list(map(plan_to_plan_response, plans))
        body = {
            'results': plans
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