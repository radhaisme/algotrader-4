"""
This module import tick data provided for FXMC
check: https://github.com/FXCMAPI/FXCMTickData

The following instruments are available:
AUDCAD,AUDCHF,AUDJPY,AUDNZD,CADCHF,EURAUD,EURCHF,EURGBP,EURJPY
EURUSD,GBPCHF,GBPJPY,GBPNZD,GBPUSD,NZDCAD,NZDCHF.NZDJPY,NZDUSD
USDCAD,USDCHF,USDJPY
"""
import datetime
import gzip
import pathlib
import sys
import time

import requests
from sqlalchemy.orm import sessionmaker

from common.config import sql_config, fxcm_data_path
from database.create import SqlEngine


def get_datafiles(list_to_download, store_path):
    """
    Connect to FXMC server and download the data requested.
    At the moment the server does not require authentication.

    Args:
        symbol: One od the available currency pairs in format XXXYYY
        year: since 2015
        week: week of the year

    Returns: Compressed file with requested data.
             Save the file to the data store.
             If file already exist in store and no overwriting occur.
    """

    # This is the base url and the file extension
    url = fxcm_data_path()
    store = pathlib.Path(store_path)
    url_suffix = '.csv.gz'

    # Set the dates
    start_dt = datetime.date(2018, 1, 1)
    end_dt = datetime.date(2018, 3, 30)

    # Dates
    start_year = start_dt.isocalendar()[0]
    end_year = end_dt.isocalendar()[0]

    # Create dict with all the required urls
    urls = {}
    for symbol in list_to_download:
        for yr in range(start_year, end_year + 1):
            # All within same year
            if start_year == end_year:
                start_wk = start_dt.isocalendar()[1]
                end_wk = end_dt.isocalendar()[1]
            else:
                # When more than a year - first year
                if yr == start_year:
                    start_wk = start_dt.isocalendar()[1]
                    end_wk = datetime.date(yr, 12, 28).isocalendar()[1] + 1
                # When more than a year - end year
                elif yr == end_year:
                    start_wk = 1
                    end_wk = end_dt.isocalendar()[1] + 1
                # When more than a year - in between
                else:
                    start_wk = 1
                    end_wk = datetime.date(yr, 12, 28).isocalendar()[1] + 1

            # Construct URLs and saving paths. Save to dictionary
            for wk in range(start_wk, end_wk):
                data_folder = store / symbol / str(yr)
                file_name = str(wk) + url_suffix

                url_in = {'url': ('{}/{}/{}/{}{}'.format(url, symbol, str(yr), str(wk),
                                                         url_suffix)),
                          'dir_path': data_folder,
                          'file_path': data_folder / file_name}

                key = symbol + str(yr) + "_" + str(wk)
                urls[key] = url_in

    # Run for every file
    err = []
    for key in urls:
        dir_path = urls[key]['dir_path']
        file_path = urls[key]['file_path']
        url = urls[key]['url']

        # Recursively creates the directory and does not raise an exception if
        # the directory already exists. See: https://goo.gl/rn3q4E
        pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
        # Check for the created directory.
        if not pathlib.Path.is_dir(dir_path):
            print("Error creating the directory: {}".format(dir_path))
            sys.exit()

        try:
            time.sleep(2)
            # Check if file already exist
            if not pathlib.Path.is_file(file_path):
                # make the request
                print('Requesting: {}'.format(url))
                r = requests.get(url, timeout=60)
                print('Response status: {}'.format(r.status_code))

                # Save the file
                if r.status_code == 200:
                    print('Saving file at: {}'.format(file_path))
                    # save the file in chunks
                    chunk_size = 5000
                    with open(file_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size):
                            f.write(chunk)
                else:
                    err.append(file_path)

            else:
                # inform file already exist
                print('file {} already in store'.format(url))

        except requests.exceptions.RequestException as e:
            print(e)
            # sys.exit(1)

    print('==================================================')
    print('Errors: {}'.format(err))
    print('==================================================')
    print('All file successfully downloaded')


def clean_fxmc_file(original_path, clean_path):
    """
    FXMC file come with invalid characters '\x00' that you must clean first
    https://goo.gl/1zoTST

    Returns: clean csv.gz file

    """
    # Get the file list of the original downloads
    orig_dir_path = pathlib.Path(original_path)
    original_files = list(orig_dir_path.glob('**/*.gz'))
    total_original_files = len(original_files)

    # Get the file list of the clean files, if any.
    clean_dir_path = pathlib.Path(clean_path)
    clean_files = list(clean_dir_path.glob('**/*.gz'))
    total_clean_files = len(clean_files)

    counter = 0
    for each_file in original_files:
        # get the components of the path
        path_parts = pathlib.Path(each_file).parts
        # Create a new path in clean directory
        clean_file_path = clean_dir_path / path_parts[-3] / path_parts[-2] / path_parts[-1]
        # Get the new parent
        new_parent_dir = pathlib.Path(clean_file_path).parent

        if clean_file_path not in clean_files:
            # Recursively creates the directory and does not raise an exception if
            # the directory already exists. See: https://goo.gl/rn3q4E
            pathlib.Path(new_parent_dir).mkdir(parents=True, exist_ok=True)

            # Open, clean and creates a new file
            with gzip.open(each_file, 'rb') as f:
                data = f.read()

            with gzip.open(clean_file_path, 'wb') as f:
                f.write(data.decode('utf-8').replace('\x00', '').encode('utf-8'))

            counter += 1
            print(
                'Doing {} out of of {} - {:.3%}'.format(counter, total_original_files, counter / total_original_files))
        else:
            counter += 1
            print('File {} already in store'.format(counter))


def format_to_sql_database(file_path):
    """
    Opens a file downloaded from FXMC and format it to upload to Securities Master database
    Returns:

    """
    pass
    # chunksize = 100000
    # for df in pd.read_csv(file_path + file_name, chunksize=chunksize, iterator=True, compression='gzip'):
    #    print(df)


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

    for instance in s.query('symbols').filter_by(symbol='EURUSD'):
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

    # print(last_update)


if __name__ == '__main__':

    # 1. Download files
    symbols = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'CADCHF', 'EURAUD',
               'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCHF', 'GBPJPY',
               'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD',
               'USDCAD', 'USDCHF', 'USDJPY']
    store_path = "/media/sf_D_DRIVE/Trading/data/fxcm"
    #get_datafiles(list_to_download=symbols)

    # 2. Clean the files for Null Characters
    saving_dir_path = "/media/sf_D_DRIVE/Trading/data/clean_fxcm"
    # clean_fxmc_file(store_path, saving_dir_path)

    # 3. Format and upload to securities master DB
    file_to_save = "/media/sf_D_DRIVE/Trading/data/clean_fxcm/EURUSD/2018/5.csv.gz"



