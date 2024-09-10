import io
import os
import pandas as pd
import re
import requests
import sqlalchemy
import zipfile

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

DB_HOST = os.environ['HOSTNAME']
DB_NAME = os.environ['DATABASE']
DB_USER = os.environ['USER']
DB_PASSWORD = os.environ['PASSWORD']

MATCHES_DIR = 'data/matches/'
MATCHES_INFO_DIR = 'data/matches_info/'

@contextmanager
def get_db_connection():
    engine = sqlalchemy.create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    connection = engine.connect()

    try:
        with connection.begin():
            yield connection
    finally:
        connection.close()
        engine.dispose()

def download_matches(matches_url):
    response = requests.get(matches_url)
    zip_file = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for info in zip_ref.infolist():
            if re.search(r"info", info.filename):
                zip_ref.extract(info, MATCHES_INFO_DIR)
            elif re.search(r"csv", info.filename):
                zip_ref.extract(info, MATCHES_DIR)

def insert_file_data(directory, file, table_name, conn):
    df = pd.read_csv(os.path.join(directory, file), header=0)
    try:
        df.to_sql(table_name, conn, schema='raw', if_exists='append', index=False)        

    except Exception:
        return

def load_files_to_db(directory, table_name, conn):
    files = os.listdir(directory)

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(insert_file_data, directory, file, table_name, conn) for file in files]
        for future in futures:
            future.result()

def load_data_to_db():
    with get_db_connection() as conn:
        
        # Load match deliveries data
        load_files_to_db(MATCHES_DIR, 'deliveries', conn)

def check_and_create_table():
    with get_db_connection() as conn:
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS raw;              
            
            CREATE TABLE IF NOT EXISTS raw.deliveries (
            match_id INT,
            season INT,
            start_date DATE,
            venue VARCHAR(255),
            innings INT,
            ball INT,
            batting_team VARCHAR(255),
            bowling_team VARCHAR(255),
            striker VARCHAR(255),
            non_striker VARCHAR(255),
            bowler VARCHAR(255),
            runs_off_bat INT,
            extras INT,
            wides INT,
            noballs INT,
            byes INT,
            legbyes INT,
            penalty INT,
            wicket_type VARCHAR(255),
            player_dismissed VARCHAR(255),
            other_wicket_type VARCHAR(255),
            other_player_dismissed VARCHAR(255)
            )
        """))

def lambda_handler(event, context):
    url = event['url']

    # download_matches(url)
    check_and_create_table()
    load_data_to_db()

    return {
        'statusCode': 200,
        'body': 'Matches downloaded successfully'
    }

lambda_handler({'url': 'https://cricsheet.org/downloads/recently_played_2_male_csv2.zip'}, None)