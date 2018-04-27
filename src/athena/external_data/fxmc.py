"""
This module import tick data provided for FXMC
check: https://github.com/FXCMAPI/FXCMTickData

The following instruments are available:
AUDCAD,AUDCHF,AUDJPY,AUDNZD,CADCHF,EURAUD,EURCHF,EURGBP,EURJPY
EURUSD,GBPCHF,GBPJPY,GBPNZD,GBPUSD,NZDCAD,NZDCHF.NZDJPY,NZDUSD
USDCAD,USDCHF,USDJPY
"""
import gzip
import pathlib
import sys
import datetime
import pandas as pd
import requests

from common import fxcm_data_path
from mysql_manager import securities_master_engine as engine


def get_datafiles(list_to_download, store_path):
    """
    Connect to FXMC server and download the data requested.
    At the moment the server does not require authentication.

    Args:
        list_to_download:
        store_path:
        year: since 2015
        week: week of the year

    Returns: Compressed file with requested data.
             Save the file to the data store.
             If file already exist in store and no overwriting occur.
    """

    # This is the base url and the file extension
    url = fxcm_data_path()
    store_path = pathlib.Path(store_path)
    url_suffix = '.csv.gz'

    # Set the dates
    start_dt = datetime.date(2015, 1, 1)
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
                data_folder = store_path / symbol / str(yr)
                file_name = symbol + '_' + str(yr) + '_' + str(wk) + url_suffix

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
            #time.sleep(2)
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
            print('Doing {} out of of {} - {:.3%}'.format(counter, total_original_files,
                  counter / total_original_files))
            print('File: {}'.format(clean_file_path))
        else:
            counter += 1
            print('File {} already in store'.format(clean_file_path))


def prepare_data_for_securities_master(file_path):
    """
    Opens a file downloaded from FXMC and format it to upload to Securities Master databases
    Returns: pandas DF

    """
    # Something to read when working with a lot of data
    # https://www.dataquest.io/blog/pandas-big-data/

    df = pd.read_csv(filepath_or_buffer=file_path,
                     compression='gzip',
                     sep=',',
                     skiprows=1,
                     names=['price_datetime', 'bid', 'ask'],
                     parse_dates=[0],
                     date_parser=pd.to_datetime,
                     index_col=[0])

    df['last_update'] = datetime.datetime.now()

    return df


def load_to_securities_master(mysql_engine, data_to_load):

    table_name = "AUDCAD_fxcm"

    data_to_load.to_sql(con=mysql_engine,
                        name=table_name,
                        if_exists='append',
                        index=True,
                        chunksize=100000)


def load_to_securities_database2(mysql_engine, csv_to_load):
    from sqlalchemy.orm import sessionmaker

    table_name = "AUDCAD_fxcm"

    SessionMaker = sessionmaker(bind=mysql_engine)
    session = SessionMaker()

    sql_stat="LOAD DATA LOCAL INFILE " + csv_to_load
    "INTO TABLE settles "
    "FIELDS TERMINATED BY ',' "
    "lines terminated by '\n' "
    "IGNORE 1 LINES "
    "(price_date_utc, bid, ask, last_update_utc);"

    session.execute(sql_stat)
    session.flush()
    engine.dispose()


def main():
    my_engine = engine()

    my_path = "/media/sf_D_DRIVE/Trading/data/example_data/AUDCAD/"
    my_path = pathlib.Path(my_path)
    all_files = my_path.glob('**/*.gz')

    count = 0
    for each_file in all_files:
        print('Working on {}'.format(each_file))
        data = prepare_data_for_securities_master(each_file)
        print(data.head())
        print('Data ready')
        load_to_securities_master(mysql_engine=my_engine, data_to_load=data)
        print('Data imported')
        count += 1
        print(count)
        print('######################################################')

    print('All Done !')


if __name__ == '__main__':
    # 1. Download files
    #symbols = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'CADCHF', 'EURAUD',
    #           'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCHF', 'GBPJPY',
    #           'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD',
    #           'USDCAD', 'USDCHF', 'USDJPY']
    #store = "/media/sf_D_DRIVE/Trading/data/fxcm"
    # get_datafiles(list_to_download=symbols, store_path=store)

    # 2. Clean the files for Null Characters
    #saving_dir_path = "/media/sf_D_DRIVE/Trading/data/clean_fxcm"
    #clean_fxmc_file(original_path=store, clean_path=saving_dir_path)

    # 3. Check files integrity after the clean up.
    main()






