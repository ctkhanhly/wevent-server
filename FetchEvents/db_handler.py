import os
import boto3

AWS_KEY="AKIAU64U76RLZT3SMF4X"
AWS_SECRET_KEY="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"

def get_venues():
    db = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    )
    table = db.Table("Venues")
    response = table.scan()
    data = response["Items"]
    venues = []
    for i in data:
        venues.append(int(i["venue_id"]))
    print(f"Returned {len(venues)} venues")
    return venues
    
