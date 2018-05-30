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
import sys
from collections import OrderedDict
from gzip import open as opengz

import pandas as pd
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from pytz import utc

from common.settings import ATSett
from data_acquisition.fxmc import in_store
from databases.influx_manager import influx_client
from log.logging import setup_logging


def series_by_filename(tag, dir_path):
    """Returns dictionary with only path for files already in database

    :param tag: 'filename'
    :param dir_path: base path
    :return: {filename: filepath
    """
    database = 'securities_master'

    cql = 'SHOW TAG VALUES ON \"{}\" WITH KEY=\"{}\"'.format(database, tag)
    try:
        client = influx_client(client_type='client', user_type='reader')
        cql_response = client.query(cql).items()
        client.close()
    except InfluxDBClientError:
        logger.exception('Could not query series in table')
        raise SystemError

    if cql_response:
        response = cql_response[0][1]
    else:
        response = cql_response

    # https://stackoverflow.com/a/39537308/3512107
    ans = OrderedDict()
    for resp in response:
        filename = resp['value']
        store_path = store_path_constructor(filename=filename, dir_path=dir_path)
        ans[filename] = store_path
    return ans


def series_in_table(table, dir_path):
    """Returns dictionary with info of all series in a given table

    :param table: table name
    :param dir_path: base path
    :return: {filename: {row_count, filepath}
    """

    cql = 'SELECT COUNT(bid) FROM {} GROUP BY filename'.format(table)
    try:
        client = influx_client(client_type='dataframe', user_type='reader')
        response = client.query(cql).items()
        client.close()
    except InfluxDBClientError:
        logger.exception('Could not query series in table')
        raise SystemError

    # https://stackoverflow.com/a/39537308/3512107
    ans = OrderedDict()
    for resp in response:
        filename = resp[0][1][0][1]
        row_count = resp[1]['count'].iloc[0]
        store_path = store_path_constructor(filename=filename,
                                            dir_path=dir_path)
        ans[filename] = {'row_count': row_count,
                         'filepath': store_path}
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
    logger.info('Preparing CSV file: %s', filename)
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
        logger.exception('Error reading file %s,', filename)
        sys.exit(-1)

    logger.info('File: %s ready for insert', filename)
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


def writer(data, tags, into_table):
    """Insert tick data into securities master database.
    :param data:
    :param tags:
    :param into_table:
    :return:
    """
    protocol = 'json'
    field_columns = ['bid', 'ask']
    logger.info('Insert data for: %s', tags['filename'])
    try:
        client = influx_client(client_type='dataframe', user_type='writer')

        client.write_points(dataframe=data,
                            measurement=into_table,
                            protocol=protocol,
                            field_columns=field_columns,
                            tags=tags,
                            time_precision='u',
                            numeric_precision='full',
                            batch_size=10000)
        client.close()
        logger.info('Data insert OK for %s', tags['filename'])
    except (InfluxDBServerError, InfluxDBClientError):
        logger.exception('Error data insert - %s', tags['filename'])
        sys.exit(-1)


def insert_validation(filepath, table, tags, abs_tolerance=10):
    """Validate number of rows: CSV vs Database

    """
    client = influx_client(client_type='dataframe', user_type='reader')

    filename = tags['filename']
    symbol = tags['symbol']
    provider = tags['provider']
    row_count = sum(1 for _r in opengz(filepath, 'r')) - 1

    cql = 'SELECT COUNT(bid) FROM {} ' \
          'WHERE filename=\'{}\' ' \
          'AND symbol=\'{}\' ' \
          'AND provider=\'{}\''.format(table, filename, symbol, provider)

    try:
        rows_in_db = client.query(query=cql)[table]['count'].iloc[0]
        client.close()
    except KeyError:
        logger.info('Data from %s not in database', filename)
        return {'value': 'Not in DB',
                'csv': row_count,
                'sec_master': 0,
                'diff': row_count}

    difference = abs(row_count - rows_in_db)
    if difference == 0:
        ans = 'Exact'
    elif difference > abs_tolerance:
        ans = 'Not Acceptable'
    else:
        ans = 'Acceptable'

    logger.info('Validation %s difference of %d', ans, difference)
    return {'value': ans,
            'csv': row_count,
            'sec_master': rows_in_db,
            'diff': difference}


def delete_series(tags):
    """Deletes series in current database
    :param tags:
    :return:
    """

    try:
        client = influx_client(client_type='client', user_type='writer')
        client.delete_series(tags=tags)
        client.close()
    except (InfluxDBClientError, InfluxDBServerError):
        logger.exception('Could not delete series %s', tags['filename'])


def get_files_to_load(dir_path, overwrite, validation_type='fast'):
    """ List of all file to be insert.
    All possible files minus already in database if overwrite is False
    :param dir_path:
    :param overwrite:
    :param validation_type:
    :return:
    """
    # Define the set of files to work with
    dir_path = pathlib.Path(dir_path)
    logger.info('Constructing list of files to insert')
    all_possible_files = in_store(dir_path)

    if overwrite:
        files = all_possible_files
    else:
        if validation_type == 'fast':
            # validates that series by tag value = filename
            already_in_db = series_by_filename(tag='filename',
                                               dir_path=dir_path)
        elif validation_type == 'full':
            logger.info('Verification what is already in database. '
                        'Be patient. !!!')
            # TODO: row_count by file validation. CSV vs DB

        # Deletes last inserted series. this is done for safety,
        # because if last time the loading function was stopped then
        # the last series could be incomplete.
        if already_in_db:
            last_insert = list(already_in_db.keys())[-1]
            delete_series(tags={'filename': last_insert})
            del already_in_db[last_insert]

        logger.info('%d files already loaded into database',
                    len(already_in_db))

        for _key, value in already_in_db.items():
            filepath = pathlib.Path(value)
            if not filepath == last_insert and filepath in all_possible_files:
                all_possible_files.remove(filepath)
        files = all_possible_files

    logger.info('%s files for insert', len(all_possible_files))
    return files


def load_multiple_tick_files(dir_path, provider, into_table, overwrite=False):
    """ Iterates over a directory and load all the .gz files with tick data
    from FXCM.
    Files must math REGEX: "^[A-Z]{6}_20\\d{1,2}_\\d{1,2}.csv.gz"
    """

    files = get_files_to_load(dir_path=dir_path, overwrite=overwrite)

    # Loop each file in directory
    for each_file in files:
        # Get some basic information about the data
        symbol = each_file.parts[-1][:6]
        filename = each_file.parts[-1][:-7]
        tags = {'symbol': symbol,
                'provider': provider,
                'filename': filename}
        logger.info('Working on %s', filename)

        # Validate if data already is in securities master database.
        # Number of data points in CSV must be similar (+/- tolerance)
        # to database to be considered as already inserted.
        pre_validation = insert_validation(filepath=each_file,
                                           table=into_table,
                                           tags=tags)

        if pre_validation['value'] == 'Not Acceptable' or \
                pre_validation['value'] == 'Not in DB':
            # deletes series with same tags if already in database
            delete_series(tags=tags)
            # turn the CSV into a dataframe ready for insert
            data = prepare_for_securities_master(file_path=each_file)
            # insert the data to sec master database
            writer(data=data, tags=tags, into_table=into_table)

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
                logger.info('Successful insert for %s: %d '
                            'data points with %d difference',
                            filename,
                            post_validation['sec_master'],
                            post_validation['diff'])
            else:
                logger.error('Error insert for %s: %s '
                             'difference', filename, post_validation['diff'])

        else:
            logger.info('Data for %s already in database:'
                        ' %d data points with %d difference', filename,
                        pre_validation['sec_master'],
                        pre_validation['diff'])

    logger.info('All data files processed!')


def multiple_file_insert():
    """

    :return:
    """
    store = pathlib.Path(ATSett().store_clean_fxcm())

    time0 = datetime.datetime.now()

    logger.info('#'*90)
    logger.info('#'*27 + ' START LOADING MULTIPLE TICK FILES ' + '#'*28)
    logger.info('#' * 90)

    load_multiple_tick_files(dir_path=store,
                             provider='fxcm',
                             into_table='fx_ticks',
                             overwrite=False)
    time1 = datetime.datetime.now()
    logger.info('TOTAL RUNNING TIME WAS: %s', (time1 - time0))


def insert_one_series():
    """

    :return:
    """
    tags = {'filename': 'GBPUSD_2016_23',
            'provider': 'fxcm',
            'symbol': 'GBPUSD'}

    filepath = pathlib.Path(
        '/media/javier/My Passport/Trading/data/clean_fxcm'
        '/LOADED/GBPUSD/2016/GBPUSD_2016_23.csv.gz')

    # deletes series with same tags if already in database
    delete_series(tags=tags)
    # turn the CSV into a dataframe ready for insert
    data = prepare_for_securities_master(file_path=filepath)
    # insert the data to sec master database
    writer(data=data, tags=tags, into_table='fx_ticks')


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('Fxcm data insert')
    multiple_file_insert()
