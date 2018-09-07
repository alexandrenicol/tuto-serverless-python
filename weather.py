import json
import logging
import os
import time
import boto3
import uuid
import requests

dynamodb = boto3.resource('dynamodb')

def collect(event, context):

    data = json.loads(event['body'])
    if 'city' not in data:
        logging.error("Validation Failed")
        raise Exception("Please include a city in the POST body.")
        return
    #data['url']

    r = requests.get('https://api.openweathermap.org/data/2.5/weather?q='+data['city']+'&APPID=8d34ccb1d21edc4f78f552845e027e56&units=metric')
    weatherData = r.json()

    timestamp = int(time.time() * 1000)

    table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])

    item = {
        'id': str(uuid.uuid1()),
        'city': data['city'],
        'createdAt': timestamp,
        'temp': str(weatherData['main']['temp'])
    }

    # write the todo to the database
    table.put_item(Item=item)

    response = {
        "statusCode": 200,
        "body": json.dumps(item)
    }

    return response
