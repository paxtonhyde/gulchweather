''' engine.py '''
import requests
import time
from datetime import date

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

WUNDERGROUND_KEY = 'e8f9bc08a1154a23b9bc08a1158a2398'
PWS = 'KNMTHORE3'
UNITS = 'm' # m = metric, e = imperial
QUERY_CURRENT = f"https://api.weather.com/v2/pws/observations/current?stationId={PWS}&format=json&units={UNITS}&apiKey={WUNDERGROUND_KEY}&numericPrecision=decimal"

def extract_date(wunderground_json):
    return date.fromisoformat(wunderground_json['observations'][0]['obsTimeLocal'][:10])

if __name__ == "__main__":
    # todo: add try/catch for 
    # requests.exceptions.ConnectionError
    # urllib3.exceptions.MaxRetryError
    try:
        result = requests.get(QUERY_CURRENT)
    except requests.exceptions.ConnectionError as ce:
        print(ce)
        time.sleep(naptime)

    naptime = 120
    flush_counter = (60*60) / naptime # flush every hour

    previous_obs_date = extract_date(result.json())
    schema = avro.schema.parse(open(f"../test_data/wunderground.avsc", "rb").read())
    writer = DataFileWriter(open(f"../test_data/{previous_obs_date.isoformat()}.avro", "ab"), DatumWriter(), schema)

    while result.status_code == 200:
    # counter = 0
    # while counter < 3:
        # todo: check for new api key
        # terminal output
        
        temp = result.json()['observations'][0]['metric']['temp']
        humidity = result.json()['observations'][0]['humidity']
        pressure = result.json()['observations'][0]['metric']['pressure']
        print(result.json()['observations'][0]['obsTimeLocal'] + f" | temp = {temp:.2f}C | {humidity}% humidity | {pressure} hPa" )
 
        writer.append(result.json()['observations'][0])
        if flush_counter <= 0:
            writer.flush()
            flush_counter = (60*60) / naptime
        else:
            flush_counter -= 1

        time.sleep(naptime)
        try:
            result = requests.get(QUERY_CURRENT)
        except requests.exceptions.ConnectionError as ce:
            print(ce)
            time.sleep(naptime)

        # check if it's a new day
        new_obs_date = extract_date(result.json()) 
        if new_obs_date != previous_obs_date:
            writer.close()
            writer = DataFileWriter(open(f"../test_data/{new_obs_date.isoformat()}.avro", "ab"), DatumWriter(), schema)

        previous_obs_date = new_obs_date

        # counter += 1

    # reader = DataFileReader(open(f"../test_data/{previous_obs_date}.avro", "rb"), DatumReader())
    # for reading in reader:
    #     print(reading)
    # reader.close()

    print("broke")