"""
This module import tick data provided for FXMC
check: https://github.com/FXCMAPI/FXCMTickData

The following instruments are available:
AUDCAD,AUDCHF,AUDJPY,AUDNZD,CADCHF,EURAUD,EURCHF,EURGBP,EURJPY
EURUSD,GBPCHF,GBPJPY,GBPNZD,GBPUSD,NZDCAD,NZDCHF.NZDJPY,NZDUSD
USDCAD,USDCHF,USDJPY
"""
from database.create import SqlEngine
from sqlalchemy.orm import sessionmaker
from common.config import sql_config, data_storage_path, fxcm_data_path
from os.path import join, isfile
import time
import pathlib
import sys
import datetime
import gzip


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

    # list_to_download = ['AUDCAD']
    # Set the dates
    start_dt = datetime.date(2015, 1, 1)
    end_dt = datetime.date(2017, 12, 28)

    # Dates
    start_year = start_dt.isocalendar()[0]
    end_year = end_dt.isocalendar()[0]

    # Create dict with all the required urls
    urls = {}
    for symbol in list_to_download:
        for yr in range(start_year, end_year+1):
            # All within same year
            if start_year == end_year:
                start_wk = start_dt.isocalendar()[1]
                end_wk = end_dt.isocalendar()[1]
            else:
                # When more than a year - first year
                if yr == start_year:
                    start_wk = start_dt.isocalendar()[1]
                    end_wk = datetime.date(yr, 12, 28).isocalendar()[1]+1
                # When more than a year - end year
                elif yr == end_year:
                    start_wk = 1
                    end_wk = end_dt.isocalendar()[1]+1
                # When more than a year - in between
                else:
                    start_wk = 1
                    end_wk = datetime.date(yr, 12, 28).isocalendar()[1]+1

            # Construct URLs and saving paths, save to dictionary
            for wk in range(start_wk, end_wk):
                url_in = {'url': ('{}/{}/{}/{}{}'.format(url, symbol, str(yr), str(wk),
                                                         url_suffix)),
                          'dir_path': join(store, 'fxcm', symbol, str(yr)),
                          'file_path': join(store, 'fxcm', symbol, str(yr), str(wk) +
                                            url_suffix)}
                key = symbol+str(yr)+"_"+str(wk)
                urls[key] = url_in

    for key in urls:
        dir_path = urls[key]['dir_path']
        file_path = urls[key]['file_path']
        url = urls[key]['url']

        # Recursively creates the directory and does not raise an exception if
        # the directory already exists. See: https://goo.gl/rn3q4E
        pathlib.Path(dir_path).mkdir(parents = True, exist_ok = True)

        # make the request
        try:
            time.sleep(12)
            if not isfile(url):
                print('Requesting: {}'.format(url))
                r = requests.get(url, timeout = 60)
                print('Response status: {}'.format(r.status_code))
            else:
                print('file {} already in store'.format(url))
        except requests.exceptions.RequestException as e:
            print(e)
            # sys.exit(1)

        err = []
        if r.status_code == 200:
            print('Saving file.'. format(file_path))
            # save the file in chunks
            chunk_size = 5000
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size):
                    f.write(chunk)
        else:
            err.append(file_path)

    print('==================================================')
    print('Errors: {}'.format(err))
    print('==================================================')
    print('All file successfully downloaded')


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
    Session.configure(bind = new_engine)

    last_update = get_last_updated_price(Session())

    # print(last_update)


if __name__ == '__main__':

    list_to_update = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'CADCHF', 'EURAUD',
                      'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCHF', 'GBPJPY',
                      'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD',
                      'USDCAD', 'USDCHF', 'USDJPY']

    get_datafiles()
