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
import requests

from common.config import fxcm_data_path


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



if __name__ == '__main__':
    # 1. Download files
    #symbols = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'CADCHF', 'EURAUD',
    #           'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCHF', 'GBPJPY',
    #           'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD',
    #           'USDCAD', 'USDCHF', 'USDJPY']
    store = "/media/sf_Trading/data/example_data/ORIGINAL"
    # get_datafiles(list_to_download=symbols, store_path=store)

    # 2. Clean the files for Null Characters
    saving_dir_path = "/media/sf_Trading/"
    clean_fxmc_file(original_path=store, clean_path=saving_dir_path)

    # 3. Check files integrity after the clean up.
    #main()






