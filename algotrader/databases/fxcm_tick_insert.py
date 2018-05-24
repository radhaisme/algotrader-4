# -*- coding: utf-8 -*-
# https://medium.com/netflix-techblog/scaling-time-series-data-storage-part-i-ec2b6d44ba39
# https://www.influxdata.com/blog/influxdb-vs-cassandra-time-series/
# Something to read when working with a lot of data
# https://www.dataquest.io/blog/pandas-big-data/
import pandas as pd
import datetime
import logging
import pathlib
import sys

from gzip import open
from influxdb.exceptions import *
from pytz import utc
from data_acquisition.fxmc import in_store
from databases.influx_manager import influx_client
from log.logging import setup_logging
from common.config import store_clean_fxcm


def prepare_data_for_securities_master(file_path):
    """Opens a file downloaded from FXCM and format it to upload to Securities Master databases

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
        sys.exit(-1)

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


def writer(data, tags, into_table):
    """Insert tick data into securities master database.
    :param data:
    :param tags:
    :param into_table:
    :return:
    """
    protocol = 'json'
    field_columns = ['bid', 'ask']
    logger.info('Insert data for: {}'.format(tags['filename']))
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
        logger.info('Data insert OK for {}'.format(tags['filename']))
    except (InfluxDBServerError, InfluxDBClientError):
        logger.exception('Error data insert - {}'.format(tags['filename']))
        sys.exit(-1)


def insert_validation(filepath, table, tags, abs_tolerance=10):
    """Validate number of rows: CSV vs Database

    """
    client = influx_client(client_type='dataframe', user_type='reader')

    filename = tags['filename']
    symbol = tags['symbol']
    provider = tags['provider']
    row_count = sum(1 for _r in open(filepath, 'r')) - 1

    cql = 'SELECT COUNT(bid) FROM {} ' \
          'WHERE filename=\'{}\' ' \
          'AND symbol=\'{}\' ' \
          'AND provider=\'{}\''.format(table, filename, symbol, provider)

    try:
        rows_in_db = client.query(query=cql)[table]['count'].iloc[0]
        client.close()
    except KeyError:
        logger.info('Data from {} not in database'.format(filename))
        return {'value': 'Not in DB', 'csv': row_count, 'sec_master': 0, 'diff': row_count}

    difference = abs(row_count - rows_in_db)
    if difference == 0:
        ans = 'Exact'
    elif difference > abs_tolerance:
        ans = 'Not Acceptable'
    else:
        ans = 'Acceptable'

    logger.info('Validation {} difference of {}'.format(ans, difference))
    return {'value': ans, 'csv': row_count, 'sec_master': rows_in_db, 'diff': difference}


def delete_series(tags):
    """Deletes series in current database
    :param tags:
    :return:
    """
    logger.info('Deleting series: {}'.format(tags['filename']))
    try:
        client = influx_client(client_type='client', user_type='writer')
        client.delete_series(tags=tags)
        client.close()
        logger.info('Series {} deleted.'.format(tags['filename']))
    except (InfluxDBClientError, InfluxDBServerError):
        logger.exception('Could not delete series {}'.format(tags['filename']))


def load_multiple_tick_files(dir_path, provider, into_table, overwrite=False):
    """ Iterates over a directory and load all the .gz files with tick data from FXCM.

        Files must math REGEX: "^[A-Z]{6}_20\d{1,2}_\d{1,2}.csv.gz"

    """

    # Define the set of files to work with
    dir_path = pathlib.Path(dir_path)
    files = in_store(dir_path)

    # Loop each file in directory
    for each_file in files:
        # Get some basic information about the data
        #
        symbol = each_file.parts[-1][:6]
        filename = each_file.parts[-1][:-7]
        tags = {'symbol': symbol, 'provider': provider, 'filename': filename}
        logger.info('Working on {}'.format(filename))

        # Validate if data already is in securities master database.
        # Number of data points in CSV must be similar (+/- tolerance) to database
        # to be considered as already inserted.
        pre_validation = insert_validation(filepath=each_file, table=into_table, tags=tags)

        if overwrite or pre_validation['value'] == 'Not Acceptable' or pre_validation['value'] == 'Not in DB':
            # deletes series with same tags if already in database
            delete_series(tags=tags)
            # turn the CSV into a dataframe ready for insert
            data = prepare_data_for_securities_master(file_path=each_file)
            # insert the data to sec master database
            writer(data=data, tags=tags, into_table=into_table)

            # Performance post insert validation that data is ok in database
            # Influx has some trouble with the milliseconds and sometimes drops some data.
            # Some tolerance is acceptable. Check the validation function for info.
            post_validation = insert_validation(filepath=each_file, table=into_table, tags=tags)

            # Post validation of inserted data
            if post_validation['value'] == 'Exact' or post_validation['value'] == 'Acceptable':
                logger.info('Successful insert for {}: {} '
                            'data points with {} difference'.format(filename, post_validation['sec_master'],
                                                                    post_validation['diff']))
            else:
                logger.error('Error insert for {}: {} difference'.format(filename, post_validation['diff']))

        else:
            logger.info('Data for {} already in database:'
                        ' {} data points with {} difference'.format(filename, pre_validation['sec_master'],
                                                                    pre_validation['diff']))

    logger.info('All data files processed!')


def multiple_file_insert():

    store = pathlib.Path(store_clean_fxcm())

    t0 = datetime.datetime.now()

    logger.info('#'*90)
    logger.info('########################### START LOADING MULTIPLE TICK FILES ############################')
    logger.info('#' * 90)

    load_multiple_tick_files(dir_path=store, provider='fxcm', into_table='fx_ticks')
    t1 = datetime.datetime.now()
    logger.info('TOTAL RUNNING TIME WAS: {}'.format(t1 - t0))


def insert_one_series():
    tags = {'filename': 'AUDNZD_2016_40',
            'provider': 'fxcm',
            'symbol': 'AUDCAD'}

    filepath = pathlib.Path(
        '/media/javier/My Passport/Trading/data/clean_fxcm/LOADED/AUDNZD/2016/AUDNZD_2016_40.csv.gz')

    # deletes series with same tags if already in database
    delete_series(tags=tags)
    # turn the CSV into a dataframe ready for insert
    data = prepare_data_for_securities_master(file_path=filepath)
    # insert the data to sec master database
    writer(data=data, tags=tags, into_table='fx_ticks')


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('FXCM LOADING INTO DATABASE')
    multiple_file_insert()

    # insert_one_series()

