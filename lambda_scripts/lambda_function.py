import io
import os
import pg8000
import re
import requests
import zipfile

from contextlib import contextmanager


DB_HOST = os.environ['HOSTNAME']
DB_NAME = os.environ['DATABASE']
DB_USER = os.environ['USER']
DB_PASSWORD = os.environ['PASSWORD']

@contextmanager
def get_db_connection():
    try:
        conn = pg8000.connect(
            host=DB_HOST,
            port=5432,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        yield conn
        conn.commit()
    except Exception as e:
        print(f"Database connection error: {e}")
        raise
    finally:
        conn.close()


def download_matches(matches_url):
    response = requests.get(matches_url)
    zip_file = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for info in zip_ref.infolist():
            if re.search(r"info", info.filename):
                zip_ref.extract(info, '/tmp/matches_info/')
            else:
                zip_ref.extract(info, '/tmp/matches/')

def load_files_to_db(folder_path, table_name, conn):
    try:
        for file_name in os.listdir(folder_path):
            with open(os.path.join(folder_path, file_name), 'r') as file:
                data = file.read().strip()  # Adjust data formatting if necessary
                sql_query = f"INSERT INTO {table_name} VALUES {data}"
                conn.run(sql_query)
                print(f"Data from {file_name} loaded into {table_name}")
    except Exception as e:
        print(f"Error loading data from {folder_path}: {e}")

def load_data_to_db():
    with get_db_connection() as conn:
        # Load match info data
        load_files_to_db('/tmp/matches_info/', 'raw.infos', conn)
        
        # Load match deliveries data
        load_files_to_db('/tmp/matches/', 'raw.deliveries', conn)

def lambda_handler(event):
    url = event['url']

    download_matches(url)
    load_data_to_db()

    return {
        'statusCode': 200,
        'body': 'Matches downloaded successfully'
    }