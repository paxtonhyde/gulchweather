''' wrapper class for making wunderground api calls '''
import requests
import time
from datetime import date

WUNDERGROUND_KEY = 'e8f9bc08a1154a23b9bc08a1158a2398'
PWS = 'KNMTHORE3'
UNITS = 'm'
QUERY_CURRENT = "https://api.weather.com/v2/pws/observations/current?stationId={pws}&format=json&units={units}&apiKey={key}&numericPrecision=decimal"


class wunderground():

    def __init__(self, pws, api_key, url, units='m'):
        '''
        url (string) – formatted with a personal weather station ID, a wunderground api key, and units (e or m, for imperial or metric)
        '''
        self.key = api_key
        self.query = url.format(pws = pws, key = self.key, units = units)

    def _load_data(self):
        '''
        for use in get function
        '''
        request_body = self.result.json()['observations'][0] # should be type dict
        self.data = {}
        for k, v in request_body.items():
            if type(v) == dict:
                for sk, sv in v.items():
                    self.data[sk] = sv
            else:
                self.data[k] = v

    def fetch_data(self, items):
        '''
        items (string or list of strings)

        returns: variable or tuple of multiple API variables
        '''
        if type(items) == str:
            return self.data[items]
        elif type(items) == list:
            stuff = []
            for k in items:
                stuff.append(self.data[k])
            return tuple(stuff)

    def get(self, fail_timer=60):
        try:
            self.result = requests.get(self.query)
        except requests.exceptions.ConnectionError as ce:
            print(f"An error occurred: {ce}")
            print(f"Trying again in {fail_timer} ...")
            time.sleep(fail_timer)
        self._load_data()
        return self.result

    def nap_then_get(self, naptime, fail_nap=60):
        time.sleep(naptime)
        self.get(fail_timer=fail_nap)
        return self.result

    def reset_key(self, new_key):
        # first make sure that a request w/ new key is succcessful
        # also want to keep track of the date when the key is reset
        # self.key = new_key
        pass

# class tests
if __name__ == "__main__":
    my_pws = wunderground(PWS, WUNDERGROUND_KEY, QUERY_CURRENT)
    print(" first request url = " + my_pws.query)

    my_pws.get()
    print(f''' first obs time = {my_pws.fetch_data('obsTimeLocal')}''')

    my_pws.nap_then_get(20)
    obs_time, temperature, stationID = my_pws.fetch_data(["obsTimeLocal", "temp", "stationID"])
    print(f''' second obs time = {obs_time} | temp = {temperature}C | stationID = '{stationID}' ''')
    