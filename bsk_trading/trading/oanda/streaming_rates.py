"""
Demonstrates streaming feature in OANDA open api

To execute, run the following command:

python streaming.py [options]

To show heartbeat, replace [options] by -b or --displayHeartBeat
"""

import requests
import json
from pprint import pprint
from common import oanda_config
import v20

def get_account_info(config):

    
    access_token = config['oanda_demo_api_key']
    account_id = '101-004-7439952-002'
    url = "https://api-fxpractice.oanda.com/v3/accounts/" + account_id + "/summary"

    s = requests.Session()
    
    headers = {'Authorization' : 'Bearer ' + access_token,
               'X-Accept-Datetime-Format' : 'unix'}
        
    params = {'accountId' : account_id}
    req = s.get(url, headers = headers)
    print(req.url)
    return req




if __name__ == "__main__":
    x = get_account_info(config=oanda_config())
    pprint(x.json())
    
