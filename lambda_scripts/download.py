import io
import os
import re
import requests
import zipfile

from sqlalchemy import create_engine

POSTGRES_URL = os.environ['POSTGRES_URL']

def download_matches(matches_url):
    response = requests.get(matches_url)
    zip_file = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for info in zip_ref.infolist():
            if re.search(r"info", info.filename):
                zip_ref.extract(info, '/tmp/matches_info/')
            else:
                zip_ref.extract(info, '/tmp/matches/')

def load_data_to_db():
    engine = create_engine(POSTGRES_URL)
    conn = engine.connect()

    for file in os.listdir('/tmp/matches_info/'):
        with open(f'/tmp/matches_info/{file}', 'r') as f:
            data = f.read()
            conn.execute(f"INSERT INTO raw.infos VALUES {data}")
    
    for file in os.listdir('/tmp/matches/'):
        with open(f'/tmp/matches/{file}', 'r') as f:
            data = f.read()
            conn.execute(f"INSERT INTO raw.deliveries VALUES {data}")

def lambda_handler(event):
    url = event['url']

    download_matches(url)
    load_data_to_db()

    return {
        'statusCode': 200,
        'body': 'Matches downloaded successfully'
    }