import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def track(data):
    track_list =[]
    for row in data['tracks']['data']:
        track_id= row['id']
        track_title=row['title']
        track_title_short=row['title_short']
        track_explicit_lyrics=row['explicit_lyrics']
        track_details={'track_id':track_id,'track_title':track_title,'track_title_short':track_title_short,'track_explicit_lyrics':track_explicit_lyrics}
        track_list.append(track_details)
    return track_list

def artist(data):
    artist_list =[]
    for row in data['tracks']['data']:
        artist_id=row['artist']['id']
        artist_name =row['artist']['name']
        artist_tracklist=row['artist']['tracklist']
        artist_details={'artist_id':artist_id,'artist_name':artist_name, 'artist_tracklist':artist_tracklist}
        artist_list.append(artist_details)
    return artist_list

def album(data):
    album_list=[]
    for row in data['tracks']['data']:
        album_id=row['album']['id']
        album_name =row['album']['title']
        album_tracklist=row['album']['tracklist']
        album_details={'album_id':album_id,'album_name':album_name,'album_tracklist':album_tracklist}
        album_list.append(album_details)
    return album_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'deezerapidata'
    Key = 'raw_data/to_processed/'
    deezer_keys = []
    deezer_data = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key=file['Key']
        if file_key.endswith('.json'):
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content=response['Body'].read().decode('utf-8')
            data=json.loads(content)
            deezer_data.append(data)
            deezer_keys.append(file_key)

    for data in deezer_data:
        track_list=track(data)
        artist_list=artist(data)
        album_list=album(data)
        track_df=pd.DataFrame(track_list)
        artist_df=pd.DataFrame(artist_list)
        album_df=pd.DataFrame(album_list)
        track_filename='transformed_data/track/' +'track_transformed_file'+str(datetime.now()) +'.csv'
        track_buffer=StringIO()
        track_df.to_csv(track_buffer,index=False)
        track_content=track_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=track_filename, Body=track_content)
        artist_filename='transformed_data/artist/'+'artist_transformed_file' +str(datetime.now()) +'.csv'
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_filename, Body=artist_content)
        album_filename='transformed_data/album/' +'album_transformed_file'+str(datetime.now()) +'.csv'
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_filename, Body=album_content)

    s3_resource = boto3.resource('s3')
    for key in deezer_keys:
        copy_source = {'Bucket': Bucket, 'Key': key}
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/'+key.split('/')[-1])
        obj = s3_resource.Object(Bucket, key)
        obj.delete()

    