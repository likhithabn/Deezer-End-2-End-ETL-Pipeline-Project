import json
import boto3
import requests
from datetime import datetime

def lambda_handler(event, context):
    # TODO implement
    playlist_id = "908622995"
    url = f"https://api.deezer.com/playlist/{playlist_id}"
    response = requests.get(url)
    data = response.json()
    print(data)
    client = boto3.client('s3')
    filename='deezer_extract' + str(datetime.now()) + '.json'
    client.put_object(Body=json.dumps(data),
     Bucket='deezerapidata',
      Key='raw_data/to_processed/'+ filename)
