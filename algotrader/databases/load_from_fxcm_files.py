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

from databases.influx_manager import influx_client
from log.logging import setup_logging


def prepare_data_for_securities_master(file_path, rows_expected):
    """Opens a file downloaded from FXMC and format it to upload to Securities Master databases


    Args:
        file_path: path

    Returns: pandas DF
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

    if df.shape[0] == rows_expected:
        logger.info('File: {} ready for insert'.format(filename))
        return df
    else:
        logger.exception('Row Count of DataFrame does not match Row Count of CSV file')
        raise ValueError()
        sys.exit(-1)


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


def writer(client, data, tags, into_table):
    """Insert tick data into securities master database.

    :param client:
    :param data:
    :param tags:
    :param into_table:
    :return:
    """
    protocol = 'json'
    field_columns = ['bid', 'ask']
    logger.info('Insert data for: {}'.format(tags['filename']))
    try:
        client.write_points(dataframe=data,
                            measurement=into_table,
                            protocol=protocol,
                            field_columns=field_columns,
                            tags=tags,
                            time_precision='u',
                            numeric_precision='full',
                            batch_size=10000)
        logger.info('Data insert OK for {}'.format(tags['filename']))
    except (InfluxDBServerError, InfluxDBClientError):
        logger.exception('Error data insert - {}'.format(tags['filename']))
        sys.exit(-1)


def insert_validation(client, filename, table, row_count, abs_tolerance=100):
    """Validate number of rows: CSV vs Database

    :param client:
    :param filename:
    :param table:
    :param row_count:
    :param abs_tolerance:
    :rtype: dict
    :return:
    """
    cql = 'SELECT COUNT(bid) FROM {} ' \
          'WHERE filename=\'{}\' '.format(table, filename)

    try:
        rows_in_db = client.query(query=cql)[table]
    except KeyError:
        logger.info('Data from {} not in database'.format(filename))
        return {'value': 'Not in DB', 'csv': row_count, 'sec_master': 0, 'diff': row_count}

    difference = row_count - rows_in_db['count'].iloc[0]
    if difference == 0:
        ans = 'Exact'
    elif abs(difference) > abs_tolerance:
        ans = 'Not Acceptable'
    else:
        ans = 'Acceptable'

    logger.info('Validation {} difference of {}'.format(ans, difference))
    return {'value': ans, 'csv': row_count, 'sec_master': rows_in_db['count'].iloc[0], 'diff': difference}


def delete_series(tags):
    """Deletes series in current database

    :param tags:
    :return:
    """
    logger.info('Deleting previous series: {}'.format(tags['filename']))
    try:
        client = influx_client()
        client.delete_series(tags=tags)
        client.close()
        logger.info('Series {} deleted.'.format(tags['filename']))
    except (InfluxDBClientError, InfluxDBServerError):
        logger.exception('Could not delete series {}'.format(tags['filename']))


def prepare_and_insert(db_client, file_path, tags, into_table, row_count, validation):

    if validation != 'Not in DB':
        # delete series with same tags in current database
        delete_series(tags)

    # Turn the CSV into a dataframe ready for insert
    data = prepare_data_for_securities_master(file_path, row_count)
    # insert the data to sec master database
    writer(client=db_client, data=data, tags=tags, into_table=into_table)


def load_multiple_tick_files(dir_path, provider, into_table, overwrite=True):
    """ Iterates over a directory and load all the .gz files with tick data from FXCM.
        Files mus be named 'XXXYYY_YYYY_w' where:
        XXXYYY = symbol's ticket
        YYYY   = year four digits
        w      = week of given year.

    :param dir_path:
    :param provider:
    :param into_table:
    :param overwrite:
    :return:
    """

    # Define the set of files to work with
    dir_path = pathlib.Path(dir_path)
    files = dir_path.glob('**/*.gz')

    # Connect to securities master database
    db_client = influx_client(client_type='dataframe')

    # Define what is considered an error (absolute difference) and define separated log for these errors.
    error_tolerance = 100
    file_errors = pd.DataFrame(columns=['filename', 'csv', 'sec_master', 'diff'])

    # Loop each file in directory
    for each_file in files:
        # Get some basic information about the data
        row_count = sum(1 for _r in open(each_file, 'r')) - 1
        symbol = each_file.parts[-1][:6]
        filename = each_file.parts[-1][:-7]
        tags = {'symbol': symbol, 'provider': provider, 'filename': filename}
        logger.info('Working on {}'.format(filename))

        # Validate if data already is in securities master database.
        # Number of data points in CSV must be similar (+/- tolerance) to database
        # to be considered as already inserted.
        pre_validation = insert_validation(client=db_client, table=into_table,
                                           row_count=row_count, filename=filename, abs_tolerance=error_tolerance)

        if overwrite or pre_validation['value'] == 'Not Acceptable' or pre_validation['value'] == 'Not in DB':
            prepare_and_insert(db_client=db_client, file_path=each_file,
                               tags=tags, into_table=into_table, row_count=row_count,
                               validation=pre_validation['value'])

            # Performance post insert validation that data is ok in database
            # Influx has some trouble with the milliseconds and sometimes drops some data.
            # Some tolerance is acceptable. Check the validation function for info.
            post_validation = insert_validation(client=db_client, table=into_table,
                                                row_count=row_count, filename=filename, abs_tolerance=error_tolerance)

            if post_validation['value'] == 'Exact' or post_validation['value'] == 'Acceptable':
                logger.info('Successful insert for {}: {} '
                            'data points with {} difference'.format(filename, row_count,
                                                                    post_validation['diff']))
            else:
                logger.error('Error insert for {}: {} difference'.format(filename, post_validation['diff']))

        else:
            logger.info('Data for {} already in database:'
                        ' {} data points with {} difference'.format(filename, row_count,
                                                                    pre_validation['diff']))

    logger.info('All data files processed!')


def multiple_file_insert():
    my_dir = r"/media/javier/My Passport/Trading/data/clean_fxcm/LOADED/"

    t0 = datetime.datetime.now()

    logger.info('#'*90)
    logger.info('########################### START LOADING MULTIPLE TICK FILES ############################')
    logger.info('#' * 90)

    load_multiple_tick_files(dir_path=my_dir, provider='fxcm', into_table='fx_ticks', overwrite=False)
    t1 = datetime.datetime.now()
    logger.info('TOTAL RUNNING TIME WAS: {}'.format(t1 - t0))


def file_insert_validation():
    csv_file_str = r"/media/javier/My Passport/Trading/data/clean_fxcm/TO_LOAD/AUDCAD_2015_17.csv.gz"
    csv_file_path = pathlib.Path(csv_file_str)
    the_filename = csv_file_path.parts[-1][:-7]
    the_table = 'fx_ticks'
    the_provider = 'fxcm'

    cql = 'SELECT * FROM {} ' \
          'WHERE filename=\'{}\' ' \
          'AND provider=\'{}\''.format(the_table,
                                       the_filename,
                                       the_provider)

    db_client = influx_client(client_type='dataframe')

    data_db = db_client.query(cql)[the_table]
    del data_db['filename']
    del data_db['provider']
    del data_db['symbol']
    data_db.index.name = 'price_datetime'
    data_db.sort_index(axis=1, inplace=True)

    data_csv = prepare_data_for_securities_master(csv_file_path)['data']
    data_csv.sort_index(axis=1, inplace=True)

    index_csv = data_csv.index
    index_db = data_db.index
    return index_csv.difference(index_db)


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)
    multiple_file_insert()
    # file_insert_validation()

