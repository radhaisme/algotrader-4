# -*- coding: utf-8 -*-
"""
https://medium.com/netflix-techblog/scaling-time-series-data-storage
-part-i-ec2b6d44ba39
https://www.influxdata.com/blog/influxdb-vs-cassandra-time-series/
Something to read when working with a lot of data
https://www.dataquest.io/blog/pandas-big-data/
"""

import datetime
import logging
import pathlib
from collections import OrderedDict
from gzip import open as opengz

import pandas as pd
from pytz import utc

import databases.influx_manager as db_man
from common.settings import AlgoSettings
from data_acquisition.fxmc import in_store
from log.log_settings import setup_logging, log_title


def series_by_filename(tag, clean_store_dirpath):
    """Returns dictionary with path for files already in database, as defined
    as filename tag present.

    :param tag: 'filename'
    :param clean_store_dirpath: base path
    :return: {filename: filepath}
    """
    database = 'sec_master'
    cql = 'SHOW TAG VALUES ON \"{}\" ' \
          'WITH KEY=\"{}\"'.format(database,
                                   tag)
    cql_response = db_man.influx_qry(cql).items()

    if cql_response:
        response = cql_response[0][1]
    else:
        response = cql_response

    # https://stackoverflow.com/a/39537308/3512107
    ans = OrderedDict()
    for resp in response:
        filename = resp['value']
        store_path = store_path_constructor(filename=filename,
                                            dir_path=clean_store_dirpath)
        ans[filename] = store_path
    return ans


def series_by_filename_row(table, clean_store_dirpath, abs_tolerance=10):
    """Returns dictionary with path for files already in database, as defined
    as filename tag present and checking row_count in database vs CSV

    :param table: table name
    :param clean_store_dirpath: base path
    :param abs_tolerance:
    :return: {filename: {row_count, filepath}
    """
    # Get the filename tags in the database
    tags_by_filename = \
        series_by_filename(tag='filename',
                           clean_store_dirpath=clean_store_dirpath)

    # For each series in database
    ans = dict()
    for each_filename, each_path in tags_by_filename.items():
        # query row count
        cql = 'SELECT COUNT(bid) ' \
              'FROM {} ' \
              'WHERE filename=\'{}\''.format(table,
                                             each_filename)
        cql_response = db_man.influx_qry(cql).items()

        row_count_db = next(cql_response[0][1])['count']
        # get row count in csv

        row_count_csv = sum(1 for _r in opengz(each_path, 'r')) - 1

        # compare the two results
        difference = abs(row_count_db - row_count_csv)
        if difference <= abs_tolerance:
            logger.info('{} already in database with {} data points and {} '
                        'difference'.format(each_filename,
                                            row_count_db,
                                            difference))
            ans[each_filename] = each_path
        else:
            logger.warning('Incomplete series {} deleted, '
                           'difference {}'.format(each_filename,
                                                  difference))
            # if difference is greater the series is incomplete, something
            # went wrong. Delete it !
            db_man.delete_series(tags={'filename': each_filename})

    return ans


def store_path_constructor(filename, dir_path):
    """ Construct a filepath object for fxcm file store in the designated
    clean store

    :param filename: regex "^[A-Z]{6}_20\\d{1,2}_\\d{1,2}" ex: "AUDCAD_2015_1"
    :param dir_path: base path
    :return: full path
    """
    store = pathlib.Path(dir_path)
    symbol = filename[:6]
    year = filename[7:11]
    full_filename = filename + '.csv.gz'
    return store / symbol / year / full_filename


def prepare_for_securities_master(file_path):
    """Opens a file downloaded from FXCM and format it to upload to
    Securities Master databases

    """

    file_path = pathlib.Path(file_path)
    filename = file_path.parts[-1][:-7]
    logger.info('Preparing CSV file: {}'.format(filename))
    try:
        df = pd.read_csv(filepath_or_buffer=file_path,
                         compression='gzip',
                         sep=',',
                         skiprows=1,
                         names=['price_datetime', 'bid', 'ask'],
                         parse_dates=[0],
                         date_parser=my_date_parser,
                         index_col=[0],
                         float_precision='high',
                         engine='c')
    except OSError:
        logger.exception('Error reading file {}'.format(filename))
        raise SystemError

    logger.info('File: {} ready for insert'.format(filename))
    return df


def my_date_parser(date_string):
    """Manual date parse
    from '%Y-%m-%d %H:%M:%s.%f'  to datetime UTC
    :param date_string:
    :return:
    """
    return datetime.datetime(int(date_string[6:10]),        # %Y
                             int(date_string[:2]),          # %m
                             int(date_string[3:5]),         # %d
                             int(date_string[11:13]),       # %H
                             int(date_string[14:16]),       # %M
                             int(date_string[17:19]),       # %s
                             int(date_string[20:]) * 1000,  # %f
                             tzinfo=utc)


def insert_validation(filepath, table, tags, abs_tolerance=10):
    """Validate number of rows: CSV vs Database
    """
    client = db_man.influx_client(client_type='dataframe', user_type='reader')

    filename = tags['filename']
    symbol = tags['symbol']
    provider = tags['provider']
    row_count = sum(1 for _r in opengz(filepath, 'r')) - 1

    cql = 'SELECT COUNT(bid) FROM {} ' \
          'WHERE filename=\'{}\' ' \
          'AND symbol=\'{}\' ' \
          'AND provider=\'{}\''.format(table,
                                       filename,
                                       symbol,
                                       provider)

    try:
        rows_in_db = client.query(query=cql)[table]['count'].iloc[0]
        client.close()
    except KeyError:
        logger.info('Data from {} not in database'.format(filename))
        return {'value': 'Not in DB', 'csv': row_count,
                'sec_master': 0, 'diff': row_count}

    difference = abs(row_count - rows_in_db)
    if difference == 0:
        ans = 'Exact'
    elif difference > abs_tolerance:
        ans = 'Not Acceptable'
    else:
        ans = 'Acceptable'

    logger.info('Validation {} difference of {}'.format(ans, difference))
    return {'value': ans, 'csv': row_count,
            'sec_master': rows_in_db, 'diff': difference}


def get_files_to_load(dir_path, table, overwrite, validation_type='fast'):
    """ List of all file to be insert.
    All possible files minus already in database if overwrite is False
    :param dir_path:
    :param table
    :param overwrite:
    :param validation_type:
    :return:
    """
    # Define the set of files to work with
    dir_path = pathlib.Path(dir_path)
    logger.info('Constructing list of files to insert')
    all_possible_files = in_store(dir_path)
    already_in_db = []

    if overwrite:
        files = all_possible_files
    else:
        if validation_type == 'fast':
            # validates that series by tag value = filename
            already_in_db = series_by_filename(tag='filename',
                                               clean_store_dirpath=dir_path)
        elif validation_type == 'full':
            logger.info('Verification what is already in database. '
                        'Be patient. !!!')
            already_in_db = series_by_filename_row(table=table,
                                                   clean_store_dirpath=dir_path)

        # Deletes last inserted series. this is done for safety,
        # because if last time the loading function was stopped then
        # the last series could be incomplete.
        if already_in_db:
            last_insert = list(already_in_db.keys())[-1]
            db_man.delete_series(tags={'filename': last_insert})
            del already_in_db[last_insert]
        else:
            last_insert = None

        logger.info('{} files already loaded '
                    'into database'.format(len(already_in_db)))

        for _key, value in already_in_db.items():
            filepath = pathlib.Path(value)
            if not filepath == last_insert and filepath in all_possible_files:
                all_possible_files.remove(filepath)
        files = all_possible_files

    logger.info('{} files for insert'.format(len(all_possible_files)))
    return files


def load_multiple_tick_files(dir_path, provider, into_table, overwrite=False,
                             validation_type='fast'):
    """ Iterates over a directory and load all the .gz files with tick data
    from FXCM.
    Files must math REGEX: "^[A-Z]{6}_20\\d{1,2}_\\d{1,2}.csv.gz"
    """

    files = get_files_to_load(dir_path=dir_path,
                              overwrite=overwrite,
                              validation_type=validation_type,
                              table=into_table)

    # Loop each file in directory
    for each_file in files:
        # Get some basic information about the data
        symbol = each_file.parts[-1][:6]
        filename = each_file.parts[-1][:-7]
        tags = {'symbol': symbol,
                'provider': provider,
                'filename': filename}
        logger.info('Working on {}'.format(filename))

        # Validate if data already is in securities master database.
        # Number of data points in CSV must be similar (+/- tolerance)
        # to database to be considered as already inserted.
        pre_validation = insert_validation(filepath=each_file,
                                           table=into_table,
                                           tags=tags)

        if pre_validation['value'] == 'Not Acceptable' or \
                pre_validation['value'] == 'Not in DB':
            # deletes series with same tags if already in database
            db_man.delete_series(tags=tags)
            # turn the CSV into a dataframe ready for insert
            data = prepare_for_securities_master(file_path=each_file)

            # insert the data to sec master database
            db_man.influx_writer(data=data,
                                 tags=tags,
                                 into_table=into_table,
                                 field_columns=['bid', 'ask'])

            # Performance post insert validation that data is ok in database
            # Influx has some trouble with the milliseconds and sometimes
            # drops some data. Some tolerance is acceptable.
            # Check the validation function for info.
            post_validation = insert_validation(filepath=each_file,
                                                table=into_table,
                                                tags=tags)

            # Post validation of inserted data
            if post_validation['value'] == 'Exact' or \
                    post_validation['value'] == 'Acceptable':
                logger.info('Successful insert '
                            'for {}: {} '
                            'data points with {} '
                            'difference'.format(filename,
                                                post_validation['sec_master'],
                                                post_validation['diff']))
            else:
                logger.error('Error insert for {}: {} '
                             'difference'.format(filename,
                                                 post_validation['diff']))

        else:
            logger.info('Data for {} already in database:'
                        ' {} data points with {} '
                        'difference'.format(filename,
                                            pre_validation['sec_master'],
                                            pre_validation['diff']))

    logger.info('All data files processed!')


def multiple_file_insert():
    """Main function for data insert of multiple files.

    :return:
    """
    store = pathlib.Path(AlgoSettings().store_clean_fxcm())
    time0 = datetime.datetime.now()

    log_title("START LOADING MULTIPLE TICK FILES")

    load_multiple_tick_files(dir_path=store,
                             provider='fxcm',
                             into_table='fx_ticks',
                             overwrite=False,
                             validation_type='fast')
    time1 = datetime.datetime.now()
    logger.info('TOTAL RUNNING TIME WAS: {}'.format(time1 - time0))


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('fxcm data insert')
    multiple_file_insert()
