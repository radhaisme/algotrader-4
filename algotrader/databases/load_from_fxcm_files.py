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


def prepare_data_for_securities_master(file_path):
    """Opens a file downloaded from FXMC and format it to upload to Securities Master databases


    Args:
        file_path: path

    Returns: pandas DF
    """

    file_path = pathlib.Path(file_path)

    row_count = sum(1 for r in open(file_path, 'r')) - 1

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
        logger.exception('Error reading file {}'.format(file_path))
        sys.exit(-1)

    if df.shape[0] == row_count:
        ans = dict()
        ans['row_count'] = row_count
        ans['file_path'] = file_path
        ans['data'] = df
        return ans
    else:
        logger.exception('Row Count of DataFrame does not match Row Count of CSV file')
        raise ValueError()
        sys.exit(-1)


def my_date_parser(date_string):
    """ Parse date string from '%Y-%m-%d %H:%M:%s.%f'  to datetime UTC

    :param date_string:
    :return:
    """
    return datetime.datetime(int(date_string[6:10]),  # %Y
                             int(date_string[:2]),  # %m
                             int(date_string[3:5]),  # %d
                             int(date_string[11:13]),  # %H
                             int(date_string[14:16]),  # %M
                             int(date_string[17:19]),  # %s
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
    try:
        client.write_points(dataframe=data,
                            measurement=into_table,
                            protocol=protocol,
                            field_columns=field_columns,
                            tags=tags,
                            # time_precision='ms',
                            numeric_precision='full',
                            batch_size=10000)
        logger.info('Data insert')
    except (InfluxDBServerError, InfluxDBClientError):
        logger.exception('Error data insert - {}'.format(tags))
        sys.exit(-1)


def insert_validation(client, filename, table, row_count):
    """Validate number of rows: CSV vs Database

    :param client:
    :param filename:
    :param table:
    :param row_count:
    :return: Exception if does not match
    """
    cql = 'SELECT COUNT(bid) FROM {} ' \
          'WHERE filename=\'{}\' '.format(table,
                                          filename)

    try:
        rows_in_db = client.query(query=cql)[table]
    except KeyError:
        logger.exception('Data from {} not in database'.format(filename))

    if row_count != rows_in_db['count'].iloc[0]:
        logger.exception('Row count does not match for {} - '
                         '{} in CSV vs {} in DB'.format(filename,
                                                        row_count,
                                                        rows_in_db['count'].iloc[0]))
        raise ValueError()


def load_multiple_tick_files(dir_path, provider, into_table):
    """ Iterates over a directory and load all the .gz files with tick data from FXCM.
        Files mus be named 'XXXYYY_YYYY_w' where:
        XXXYYY = symbol's ticket
        YYYY   = year four digits
        w      = week of given year.

    :param dir_path:
    :param provider:
    :param into_table:
    :return:
    """

    db_client = influx_client(client_type='dataframe')
    dir_path = pathlib.Path(dir_path)
    files = dir_path.glob('**/*.gz')

    for each_file in files:
        symbol = each_file.parts[-1][:6]
        filename = each_file.parts[-1][:-7]

        logger.info('Working on {}'.format(filename))

        tags = {'symbol': symbol,
                'provider': provider,
                'filename': filename}

        data = prepare_data_for_securities_master(each_file)

        writer(client=db_client,
               data=data['data'],
               tags=tags,
               into_table=into_table)

        insert_validation(client=db_client,
                          table=into_table,
                          row_count=data['row_count'],
                          filename=filename)

        logger.info('Writer insert {} rows from {}'.format(data['row_count'], filename))

    logger.info('All data files inserted correctly!')


def main():
    t0 = datetime.datetime.now()
    my_dir = r"/media/javier/My Passport/Trading/data/clean_fxcm/TO_LOAD/"
    logger.info('#'*90)
    logger.info('########################### START LOADING MULTIPLE TICK FILES ############################')
    logger.info('#' * 90)
    load_multiple_tick_files(dir_path=my_dir, provider='fxcm', into_table='fx_ticks')
    t1 = datetime.datetime.now()

    logger.info('TOTAL RUNNING TIME WAS: {}'.format(t1 - t0))


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)
    main()
