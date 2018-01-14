"""
This module import tick data provided for FXMC
check: https://github.com/FXCMAPI/FXCMTickData

The following instruments are available:
AUDCAD,AUDCHF,AUDJPY,AUDNZD,CADCHF,EURAUD,EURCHF,EURGBP,EURJPY
EURUSD,GBPCHF,GBPJPY,GBPNZD,GBPUSD,NZDCAD,NZDCHF.NZDJPY,NZDUSD
USDCAD,USDCHF,USDJPY
"""
from io import BytesIO
from database.create import SqlEngine
from sqlalchemy.orm import sessionmaker
from common.config import sql_config
import gzip

import urllib.request
import datetime


def get_data(symbol, year, week):
    """
    Connect to FXMC server and download the data requested.
    At the moment the server does not require authentication.

    Args:
        symbol: One od the available currency pairs in format XXXYYY
        year: since 2015
        week: week of the year

    Returns: compress file with requested data

    """
    # This is the base url and the file extension
    url = 'https://tickdata.fxcorporate.com/'
    url_suffix = '.csv.gz'

    url_data = '{}{}/{}/{}{}'.format(url, symbol, str(year), str(week), url_suffix)
    print(url_data)
    requests = urllib.request.urlopen(url_data)
    buf = BytesIO(requests.read())
    f = gzip.GzipFile(fileobj=buf)
    data = f.read()
    print(len(data))


def format_to_sql_database():
    pass


def check_local_db(instrument):
    """

    Args:
        instrument:

    Returns: last_updated_date

    """
    pass


def get_last_updated_price(db_session):
    s = db_session
    print(s)

    for instance in s.query('symbols').filter_by(symbol = 'EURUSD'):

        print(instance)



def main(instruments):
    # Create sql engine
    config = sql_config()
    conn = SqlEngine()
    conn.load_configuration(**config)
    new_engine = conn.create_engine()

    Session = sessionmaker()
    Session.configure(bind=new_engine)

    last_update = get_last_updated_price(Session())

    #print(last_update)



if __name__ == '__main__':
    list_to_update = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'CADCHF', 'EURAUD',
                      'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCHF', 'GBPJPY',
                      'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD',
                      'USDCAD', 'USDCHF', 'USDJPY']

    main(list_to_update)

