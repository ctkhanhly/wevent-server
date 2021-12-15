import requests
import json
import datetime
import os
import db_handler as db
import boto3

token = "E5FDYDO67JK2ANKIOO7L"
sqs = boto3.client('sqs')

def get_categories():
    headers={
        "Authorization": f"Bearer {token}"
    }
    response = json.loads(requests.get(f"https://www.eventbriteapi.com/v3/categories/", headers=headers).content)["categories"]
    categories = {}
    for c in response:
        categories[c["id"]] = c["name"]
    
    return categories

categories = get_categories()

def create_event(e):

    try:
        event = {
            "event_id" : e['id'],
            "name" : e['name']['text'],
            "desc" : e['description']['text'],
            "start" : e['start']['utc'],
            "end" : e['end']['utc'],
            "venue_id" : e['venue_id'],
            "category" : categories[e['category_id']],
            "summary" : e['summary'],
            "img" : e['logo']['original']['url']
        }
    except:
        event = {
            "event_id" : e['id'],
            "name" : e['name']['text'],
            "desc" : e['description']['text'],
            "start" : e['start']['utc'],
            "end" : e['end']['utc'],
            "venue_id" : e['venue_id'],
            "category" :"Unkown",
            "img" : "N/A",
            "summary" : ""
    }

    return event    
    
def get_events(venues):
    headers={
        "Authorization": f"Bearer {token}"
    }
    params = {
        'order_by' : 'start_desc'
    }
    today = str(datetime.datetime.today())
    events = []
    
    for venue_id in venues:
        if venue_id == 0:
            continue
        print(f"Getting events for {venue_id}")
        response = requests.get(f"https://www.eventbriteapi.com/v3/venues/{venue_id}/events/", headers=headers, params=params)
        venue_events = json.loads(response.content)['events']                   

        for e in venue_events:                                       
                event = create_event(e)
                if event["start"] < today:
                    break
                events.append(event) 

        if len(events) > 100:
            queue(events)
            events = []
    
    if len(events) > 0:
        queue(events)

    return events

def queue(data):    
    sqs.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/341225501783/fetch_queue",
        MessageBody=json.dumps(data)
    )


def lambda_handler(event, context):
    venues = db.get_venues()
    events = get_events(venues)
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    } 
