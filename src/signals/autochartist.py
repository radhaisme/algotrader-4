"""

Uses REST API until Oanda publish V20 version

"""
from common.oandapy import API
import oandapy
import os
import yaml
from pprint import pprint
from tabulate import tabulate

# Get configuration path from enviromental variable
CONFIG = os.environ['TRADE_CONF']


def main():

    stream = open(os.path.expanduser(CONFIG), 'r')
    config = yaml.load(stream)['oanda_demo']

    token = config.get('token')

    oanda = API(environment = "practice", access_token = token)
    payload = {'instrument': 'EUR_USD'}

    signals = oanda.get_commitments_of_traders(params = payload)
    print(tabulate(signals))


if __name__ == "__main__":
    main()

