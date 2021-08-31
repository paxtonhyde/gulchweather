''' engine.py –
    retrieves weather data from Wunderground API, serializes in Avro, writes data to ?
    
    todo:
    - use wunderground class
    - clean up while loop versus try/catch
    - write to cloud storage
    - check for new api key from website
    '''
import requests
import time
from datetime import date, datetime
import urllib3

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from wunderground import WUNDERGROUND_KEY, PWS
QUERY_CURRENT = f"https://api.weather.com/v2/pws/observations/current?stationId={PWS}&format=json&units={'m'}&apiKey={WUNDERGROUND_KEY}&numericPrecision=decimal"

from google.cloud import storage
from gcloud_vars import GOOGLE_PROJ_ID, BUCKET_NAME

def extract_date(wunderground_json):
    return date.fromisoformat(wunderground_json['observations'][0]['obsTimeLocal'][:10])

def extract(wunderground_json, field, units=None):
    # iterate through json file to find fields
    pass


if __name__ == "__main__":

    schema = avro.schema.parse(open(f"../test_data/wunderground.avsc", "rb").read())
    writer = DataFileWriter(open(f"../test_data/{date.today().isoformat()}.avro", "ab"), DatumWriter(), schema)

    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)

    naptime = 120
    flush_countdown = (60*60) / naptime # hourly
    # is this even a good parameter for continuing the loop?
    response_status = 200
    last_obs_date = date.today().isoformat()
    while response_status == 200:

        try:
            response = requests.get(QUERY_CURRENT)
        except (requests.exceptions.ConnectionError, urllib3.exceptions.MaxRetryError) as e:
            print(e)
            time.sleep(naptime)
            continue
        # will it ever break if I include this? (will it ever give a status other than 200 if I skip exceptions)
        response_status = response.status_code

        # check if it's a new day to start a new avro file
        today = extract_date(response.json()) 
        if today != last_obs_date:
            writer.close()
            # upload data to cloud?
            writer = DataFileWriter(open(f"../test_data/{today.isoformat()}.avro", "ab"), DatumWriter(), schema)
        last_obs_date = today
        
        # printing to std out
        temp = response.json()['observations'][0]['metric']['temp']
        humidity = response.json()['observations'][0]['humidity']
        pressure = response.json()['observations'][0]['metric']['pressure']
        print(response.json()['observations'][0]['obsTimeLocal'] + f" | temp = {temp:.2f}C | {humidity}% humidity | {pressure} hPa" )
        # reader = DataFileReader(open(f"../test_data/{previous_obs_date}.avro", "rb"), DatumReader())
        # for reading in reader:
        #     print(reading)
        # reader.close()

        writer.append(response.json()['observations'][0])
        if flush_countdown <= 0:
            writer.flush()
            flush_countdown = (60*60) / naptime # hourly
            new_blob = bucket.blob(f'{today.isoformat()}.avro')
            new_blob.upload_from_filename(f'../test_data/{new_blob.name}')
        else:
            flush_countdown -= 1

        time.sleep(naptime)

    print(f'response status code = {response_status} –> engine.py broke at {datetime.now()}')
    print(f'\n {response.json()}')
