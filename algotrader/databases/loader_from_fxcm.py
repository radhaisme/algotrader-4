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
    file_path = pathlib.Path(file_path)
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
        df.to_csv("D:\Trading\data\clean_fxcm\TO_LOAD\AUDCAD_2015_1.csv")
        return df

    except OSError:
        logger.exception('Error reading file {}'.format(file_path))
        sys.exit(-1)


def my_date_parser(date_string):
    return datetime.datetime(int(date_string[6:10]),        # %Y
                             int(date_string[:2]),          # %m
                             int(date_string[3:5]),         # %d
                             int(date_string[11:13]),       # %H
                             int(date_string[14:16]),       # %M
                             int(date_string[17:19]),       # %s
                             int(date_string[20:]) * 1000,  # %f
                             tzinfo=utc)


def writer(client, data, tags, into_table):
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
                            time_precision='u',
                            numeric_precision='full',
                            batch_size=10000)
        logger.info('Data insert correct')
    except (InfluxDBServerError, InfluxDBClientError):
        logger.exception('Error data insert - {}'.format(tags))
        sys.exit(-1)


def record_exist(client, file_path, table, symbol, provider):
    """
    Find if first timestamp in CSV is already in database
    Args:
        client: Influxdb client
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
    # We are going to look for records within the same second as the first row in the input file.
    # the reason for this is that Influx does not keep the ms resolution of the datetime. In some
    # cases add / subtract a nanosecond turning 00:01.290 into 00:01:289999 or similar.
    # With the approach here implemented the worst case is less that one second error.
    first_datetime_up = df.first_valid_index().strftime('%Y-%m-%d %H:%M:%S.000')
    first_datetime_down = df.first_valid_index().strftime('%Y-%m-%d %H:%M:%S.999')

    try:
        ans = client.query('SELECT * FROM {} '
                           'WHERE symbol=\'{}\' '
                           'AND provider=\'{}\' '
                           'AND time>=\'{}\' '
                           'AND time<=\'{}\''.format(table,
                                                     symbol,
                                                     provider,
                                                     first_datetime_up,
                                                     first_datetime_down))[table]

        if (ans.first_valid_index() - df.first_valid_index()) < datetime.timedelta(seconds=1):
            return True
        else:
            return False
    except KeyError:
        return False


def load_multiple_tick_files(dir_path, provider, into_table, overwrite=False):
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
            writer(client=db_client,
                   data=df,
                   tags=tags,
                   into_table=into_table)

    logger.info('All files loaded')


def main():

    t0 = datetime.datetime.now()
    my_dir = r"D:\Trading\data\clean_fxcm\LOADED"
    load_multiple_tick_files(dir_path=my_dir, provider='fxcm', into_table='fx_ticks')
    t1 = datetime.datetime.now()

    logger.info('TOTAL RUNNING TIME WAS: {}'.format(t1-t0))


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)
    main()
