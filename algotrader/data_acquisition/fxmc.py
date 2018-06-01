"""
This module import tick data provided for FXCM
check: https://github.com/FXCMAPI/FXCMTickData
"""
import gzip
import pathlib
import sys
import datetime
import requests
import logging
import re
from time import sleep
from common.settings import ATSett
from log.log_settings import setup_logging

# Available symbols from FXCM server.
SYMBOLS = ['AUDCAD', 'AUDCHF', 'AUDJPY', 'AUDNZD', 'CADCHF', 'EURAUD',
           'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCHF', 'GBPJPY',
           'GBPNZD', 'GBPUSD', 'NZDCAD', 'NZDCHF', 'NZDJPY', 'NZDUSD',
           'USDCAD', 'USDCHF', 'USDJPY']

setts = ATSett()


def in_store(store_path):
    """Return list of files that match the predefined REGEX inside the
    originals store.

    :return: matches_lst, no_matches_lst
    """
    dir_path = pathlib.Path(store_path)
    files = dir_path.glob('**/*.*')

    match = []
    pattern = re.compile("^[A-Z]{6}_20\d{1,2}_\d{1,2}.csv.gz")
    for filepath in files:
        filename = filepath.parts[-1]
        if pattern.match(filename):
            match.append(filepath)

    return match


def all_possible_urls(end_date):
    """Returns urls and saving path for all possible files to download

    :param end_date:
    :return:
    """
    # This is the base url and the file extension
    url = setts.fxcm_data_path()
    store_path = pathlib.Path(setts.store_originals_fxcm())
    url_suffix = '.csv.gz'

    # Set the dates
    start_dt = datetime.date(2015, 1, 1)
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    # years
    start_year = start_dt.isocalendar()[0]
    end_year = end_dt.isocalendar()[0]

    # Create dict with all the required urls
    urls = {}
    for symbol in SYMBOLS:
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

                urls[file_name] = url_in

    return urls


def definitive_urls(overwrite, end_date):
    """Remove urls for files already in store if overwrite False

    :param overwrite: bol
    :param end_date: str
    :return: urls dictionary
    """
    logger.info('Building urls dictionary')
    # Construct all possible URL and saving paths since 2015-01-01 until the
    # end date.
    possible_urls = all_possible_urls(end_date)

    if not overwrite:
        # What files are already in store
        already_in_store = in_store(setts.store_originals_fxcm())

        for filepath in already_in_store:
            filename = filepath.parts[-1]
            if filename in possible_urls:
                del possible_urls[filename]
    logger.info('Urls dictionary ready')
    return possible_urls


def get_files(urls):
    """Get the files for a set of urls from fxcm server

    :param urls: dic with url and saving paths
    :return:
    """
    logger.info('Request for files started. '
                '{} files in queue'.format(len(urls)))

    # Run for every file
    for key in urls:
        dir_path = urls[key]['dir_path']
        file_path = urls[key]['file_path']
        url = urls[key]['url']

        # Recursively creates the directory and does not raise an exception if
        # the directory already exists. See: https://goo.gl/rn3q4E
        pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
        # Check for the created directory.
        if not pathlib.Path.is_dir(dir_path):
            logger.exception("Error creating "
                             "the directory: {}".format(dir_path))
            sys.exit(-1)

        try:
            sleep(2)
            # make the request
            logger.info('Requesting: {}'.format(url))
            r = requests.get(url, timeout=60)
            logger.info('Response status: {}'.format(r.status_code))

            # Save the file
            if r.status_code == 200:
                logger.info('Saving file at: {}'.format(file_path))
                # save the file in chunks
                chunk_size = 5000
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size):
                        f.write(chunk)
            else:
                logger.exception('Could not get '
                                 'file: {} - '
                                 'status {}'.format(file_path,
                                                    r.status_code))
                sys.exit(-1)
        except requests.exceptions.RequestException as e:
            logger.exception('Error requesting: {}'.format(url))
            sys.exit(-1)

    logger.info('All files processed')


def clean_fxcm_originals(original_dirpath, clean_dirpath):
    """
    FXCM file come with invalid characters '\x00' that you must clean first
    https://goo.gl/1zoTST

    Returns: clean csv.gz file

    """
    # Get the file list of the original downloads
    orig_dir_path = pathlib.Path(original_dirpath)
    original_files = list(orig_dir_path.glob('**/*.gz'))
    total_original_files = len(original_files)

    # Get the file list of the clean files, if any.
    clean_dir_path = pathlib.Path(clean_dirpath)
    clean_files = list(clean_dir_path.glob('**/*.gz'))

    counter = 0
    for each_file in original_files:
        # get the components of the path
        path_parts = pathlib.Path(each_file).parts
        # Create a new path in clean directory
        clean_file_path = clean_dir_path / path_parts[-3] / path_parts[-2] / \
                          path_parts[-1]
        # Get the new parent
        new_parent_dir = pathlib.Path(clean_file_path).parent

        if clean_file_path not in clean_files:
            # Recursively creates the directory and does not raise an
            # exception if the directory already exists.
            # See: https://goo.gl/rn3q4E
            pathlib.Path(new_parent_dir).mkdir(parents=True, exist_ok=True)

            # Open, clean and creates a new file
            with gzip.open(each_file, 'rb') as f:
                data = f.read()

            with gzip.open(clean_file_path, 'wb') as f:
                f.write(
                    data.decode('utf-8').replace('\x00', '').encode('utf-8'))

            counter += 1
            logger.info('Doing {} '
                        'out of {} - '
                        '{:.3%}'.format(counter,
                                        total_original_files,
                                        counter / total_original_files))
            logger.info('Clean file: {}'.format(clean_file_path))
        else:
            counter += 1
            logger.info('Doing {} '
                        'out of {} - '
                        '{:.3%}'.format(counter, total_original_files,
                                        counter / total_original_files))
            logger.info('File {} already in store'.format(clean_file_path))


def update_all(final_date):
    """Main update function for FXCM files
    Download and clean files from server and save then in the appropriate
    location. Run from 2014-01-01 as provided by fxcm

    :param final_date: date to run to
    :return: updated original and clean directories.
    """
    urls = definitive_urls(overwrite=False,
                           end_date=final_date)
    get_files(urls)

    clean_fxcm_originals(original_dirpath=setts.store_originals_fxcm(),
                         clean_dirpath=setts.store_originals_fxcm())


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('fxcm download')

    my_date = '2018-04-30'
    update_all(my_date)




