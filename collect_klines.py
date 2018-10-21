#!/usr/bin/env python
import requests
import time
import datetime
import logging
import redis
import json

__author__ = "Adilcan Eren"
__license__ = "General Public License version 3"
__version__ = "0.0.1"
__email__ = "adilcan@riseup.net"
__status__ = "Development"

logging.basicConfig(filename='logs.log',level=logging.DEBUG)

r = redis.Redis(
    host='127.0.0.1',
    port=6379, 
    password='insert password here')

launch_epoch = 1502942400000
base_url = 'https://api.binance.com/api/v1/'

def kline_range(start, end, step):
    while start <= end:
        yield start
        start += step
        
def epoch_to_time(x):
    s = datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
    return s

def fetch_data():
    now = int(round(time.time() * 1000))
    for x in kline_range(launch_epoch, now, 60000000):
        
        print('Fetching data...')
        endpoint = base_url + 'klines?symbol=BTCUSDT&interval=1m&startTime=' + str(x) + '&limit=1000'
        current = 'Currently fetcing data of ' + epoch_to_time(x)
        print(current)
        logging.info(current)
        req = requests.get(endpoint)
        if req.status_code == 200:
            klines = req.json()
            for x in klines:
                # Returned JSON has no property names.
                k = {'open_time':x[0], 'open_value':x[1], 'high':x[2], 'low':x[3], 'close':x[4], 'volume':x[5], 'close_time':x[6], 'quote_asset':x[7], 'num_of_trades':x[8], 'buy_base_volume':x[9], 'buy_quote_volume':x[10]}
                json_k = json.dumps(k)
                r.set(x[0], json_k)
            time.sleep(60)
        elif req.status_code == 429 or 403:
            logging.warn('Violated Binance API rules, breaking... %s' % str(x))
            break
        else:        
            logging.warn('Can not perform request for kline with: %s epoch. Error code is: %s' % str(x) % str(req.status_code))

def main():
    fetch_data()
    
if __name__== "__main__":
        main()
