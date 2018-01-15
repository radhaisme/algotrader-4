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
from common.config import sql_config, data_storage_path, fxcm_data_path
from os.path import join
import time
import pathlib
import sys
import datetime
import gzip
from pprint import pprint
import urllib.request
import datetime
import requests


def get_datafiles():
    """
    Connect to FXMC server and download the data requested.
    At the moment the server does not require authentication.

    Args:
        symbol: One od the available currency pairs in format XXXYYY
        year: since 2015
        week: week of the year

    Returns: compress file with requested data.

    """
    # This is the base url and the file extension
    url = fxcm_data_path()
    store = data_storage_path()
    url_suffix = '.csv.gz'
    list_to_download = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'CADCHF', 'EURAUD',
                        'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCHF', 'GBPJPY',
                        'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD',
                        'USDCAD', 'USDCHF', 'USDJPY']
    list_to_download = ['AUDCAD']
    # Set the dates
    start_dt = datetime.date(2015, 1, 1)
    end_dt = datetime.date(2018, 2, 28)

    # Find the corresponding weeks
    start_wk = start_dt.isocalendar()[1]
    end_wk = end_dt.isocalendar()[1]
    start_year = start_dt.isocalendar()[0]
    end_year = end_dt.isocalendar()[0]

    # Create list with all the required urls
    urls =[]
    for symbol in list_to_download:
        # Loop when download is all in the same year.
        if start_year == end_year:
            for wk in range(start_wk, end_wk + 1):
                urls.append('{}/{}/{}/{}{}'.format(url, symbol, str(start_year), str(wk),
                                                   url_suffix))
        # Loop when download has more than one year
        else:
            for yr in range(start_year, end_year + 1):
                # Initial year
                if yr == start_year:
                    for wk in range(start_wk, datetime.date(yr, 12, 28).isocalendar()[
                                                  1] + 1):
                        urls.append('{}/{}/{}/{}{}'.format(url, symbol, str(yr), str(wk),
                                                           url_suffix))
                # End year
                elif yr == end_year:
                    for wk in range(1, end_wk + 1):
                        urls.append('{}/{}/{}/{}{}'.format(url, symbol, str(yr), str(wk),
                                                           url_suffix))
                # And in between
                else:
                    for wk in range(1, datetime.date(yr, 12, 28).isocalendar()[1] + 1):
                        urls.append('{}/{}/{}/{}{}'.format(url, symbol, str(yr), str(wk),
                                                           url_suffix))

            pprint(urls)
            # save_dir = join(store, 'fxcm', symbol, str(start_year))
            # save_to = join(store, 'fxcm', symbol, str(start_year), str(wk)+url_suffix)
            #
            # # Recursively creates the directory and does not raise an exception if
            # # the directory already exists. See: https://goo.gl/rn3q4E
            # pathlib.Path(save_dir).mkdir(parents = True, exist_ok = True)
            #
            # # make the request
            # try:
            #     time.sleep(5)
            #     r = requests.get(url_data)
            # except requests.exceptions.RequestException as e:
            #     print(e)
            #     sys.exit(1)


            # save the file in chunks
            #chunk_size = 2000
            #with open(save_to, 'wb') as f:
            #    for chunk in r.iter_content(chunk_size):
            #        f.write(chunk)





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



def to_do(instruments):
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

    get_datafiles()

