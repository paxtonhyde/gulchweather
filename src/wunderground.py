''' wrapper class for making wunderground api calls '''
import requests
import time
from datetime import date

WUNDERGROUND_KEY = 'e8f9bc08a1154a23b9bc08a1158a2398'
PWS = 'KNMTHORE3'
UNITS = 'm'
QUERY_CURRENT = "https://api.weather.com/v2/pws/observations/current?stationId={pws}&format=json&units={unit}&apiKey={key}&numericPrecision=decimal"


class wunderground():

    def __init__(self, pws, api_key, url, units='m'):
        '''
        url (string) – formatted with a personal weather station ID, a wunderground api key, and units (e or m, for imperial or metric)
        '''
        self.key = api_key
        self.query = url.format(pws = pws, key = self.key, units = units)

    def get(self):
        self.result = requests.get(self.query)
        return self.result

    def reset_key(self, new_key):
        pass
