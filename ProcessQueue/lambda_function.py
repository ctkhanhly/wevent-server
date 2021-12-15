import os
import boto3
import json

AWS_KEY="AKIAU64U76RLZT3SMF4X"
AWS_SECRET_KEY="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"
   
def write_events(events):
    db = boto3.resource(
        'dynamodb',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        )
    table = db.Table('Events')    
    for e in events:
        table.put_item(
            Item={
                "event_id": int(e["event_id"]),
                "event_name" : e["name"],
                "description": e["desc"],
                "start": e["start"],
                "end": e["end"],
                "venue_id": e["venue_id"],
                "category": e["category"],
                "summary": e["summary"],
                "img_url" : e["img"],
            }
        )
    
    pass


def lambda_handler(event, context):
    events = json.loads(event["Records"][0]["body"])
    write_events(events)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Handled {len(events)} items')
    } 
