# -*- coding: utf-8 -*-
# https://medium.com/netflix-techblog/scaling-time-series-data-storage-part-i-ec2b6d44ba39
# https://www.influxdata.com/blog/influxdb-vs-cassandra-time-series/

import logging
import pathlib
import sys

import datetime
import pandas as pd
from influxdb.exceptions import *
from pytz import utc

from databases.influx_manager import influx_client
from log.logging import setup_logging


def prepare_data_for_securities_master(file_path):
    """
    Opens a file downloaded from FXMC and format it to upload to Securities Master databases
    Returns: pandas DF

    """
    # Something to read when working with a lot of data
    # https://www.dataquest.io/blog/pandas-big-data/

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
        logger.info('File ready: {}'.format(file_path))
        return df
    except OSError:
        logger.exception('Error reading file {}'.format(file_path))
        sys.exit(-1)


def my_date_parser(date_string):
    """
    The default parser or pd.to_datetime are very slow.
    Since the input format is always the same better to do it manually

    https://stackoverflow.com/a/29882676/3512107
    https://stackoverflow.com/a/29882676/3512107

    Args:
        date_string: in the format MM/DD/YYYY HH:MM:SS.mmm

    Returns: datetime object with format YYYY-MM-DD HH:MM:SS.mmm

    """
    try:
        return datetime.datetime(int(date_string[6:10]),
                                 int(date_string[0:2]),
                                 int(date_string[3:5]),
                                 int(date_string[11:13]),
                                 int(date_string[14:16]),
                                 int(date_string[17:19]),
                                 int(date_string[20:23]) * 1000,
                                 tzinfo=utc)
    except Exception:
        logger('Date parser error - {}'.format(date_string))
        sys.exit(-1)


def write_to_db_with_dataframe(client, data, tags, into_table):
    """

    Args:
        client:
        data:
        tags:
        into_table:

    Returns:

    """
    protocol = 'json'
    field_columns = ['bid', 'ask']

    try:
        client.write_points(dataframe=data,
                            measurement=into_table,
                            protocol=protocol,
                            field_columns=field_columns,
                            tags=tags,
                            time_precision='ms',
                            numeric_precision='full',
                            batch_size=10000)
        logger.info('Data insert correct')
    except (InfluxDBServerError, InfluxDBClientError):
        logger.exception('Error data insert - {}'.format(tags))
        sys.exit(-1)


def record_exist(client, file_path, table, symbol, provider):
    """
    Find if firsr timestamp in CSV is already in database
    Args:
        client: Influc DF client
        file_path: path
        table: str
        symbol: str
        provider: str

    Returns: bol

    """

    # read first row od csv
    try:
        df = pd.read_csv(filepath_or_buffer=file_path,
                         compression='gzip',
                         sep=',',
                         skiprows=1,
                         names=['price_datetime', 'bid', 'ask'],
                         parse_dates=[0],
                         date_parser=my_date_parser,
                         index_col=[0],
                         nrows=1,
                         engine='c')
    except OSError:
        logger.exception('Error reading file {}'.format(file_path))
        sys.exit(-1)

    first_datetime = df.first_valid_index().strftime('%Y-%m-%d %H:%M:%S.%f')

    try:
        client.query('SELECT * FROM {} '
                     'WHERE symbol=\'{}\' '
                     'AND provider=\'{}\' '
                     'AND time=\'{}\''.format(table, symbol, provider, first_datetime))[table]
        return True
    except KeyError:
        return False


def load_multiple_tick_files(dir_path, provider, into_table, overwrite):
    """

    Args:
        dir_path: path
        provider: str
        into_table: str
        overwrite: bol
    Returns:

    """

    db_client = influx_client(client_type='dataframe')
    dir_path = pathlib.Path(dir_path)
    files = dir_path.glob('**/*.gz')

    for each_file in files:
        logger.info('Working on {}'.format(each_file))

        symbol = each_file.parts[-1][0:6]
        tags = {'symbol': symbol,
                'provider': provider}

        if overwrite:
            in_database = False
        else:
            in_database = record_exist(db_client, each_file, into_table, symbol, provider)

        if in_database:
            logger.info('Already in database {}-{} in {}'.format(symbol, provider, each_file))
        else:
            df = prepare_data_for_securities_master(each_file)
            write_to_db_with_dataframe(client=db_client,
                                       data=df,
                                       tags=tags,
                                       into_table=into_table)


    logger.info('All files loaded')


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)

    my_dir = r"D:\Trading\data\clean_fxcm\LOADED"
    load_multiple_tick_files(dir_path=my_dir, provider='fxcm', into_table='fx_tick', overwrite=False)


    # gen = query_to_db().get_points()
    # for x in gen:
    #    print(x)

    #my_path = "/media/sf_Trading/data/example_data/small.csv.gz"
    #x = prepare_data_for_securities_master(my_path)
    #print(x.head(10))
    #write_to_db(my_client, x, 'eurusd')
    #ans = query_to_db(my_client)
    #print(ans)
