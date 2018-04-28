# -*- coding: utf-8 -*-
# https://medium.com/netflix-techblog/scaling-time-series-data-storage-part-i-ec2b6d44ba39
# https://www.influxdata.com/blog/influxdb-vs-cassandra-time-series/

import pathlib
import time
import datetime
import pandas as pd
from databases.influx_manager import influx_client
from pytz import utc


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
                     date_parser=my_date_parser,
                     index_col=[0],
                     float_precision='high',
                     engine='c')
    return df


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
    return datetime.datetime(int(date_string[6:10]),
                             int(date_string[0:2]),
                             int(date_string[3:5]),
                             int(date_string[11:13]),
                             int(date_string[14:16]),
                             int(date_string[17:19]),
                             int(date_string[20:23]) * 1000,
                             tzinfo=utc)


def write_to_db(db_client, df, symbol):
    """

    Args:
        db_client:
        df:
        symbol:

    Returns:

    """

    protocol = 'json'
    field_columns = ['bid', 'ask']
    tags = {'symbol': symbol,
            'provider': 'fxcm'}
    table = 'fx_tick'

    db_client.write_points(dataframe=df,
                           measurement=table,
                           protocol=protocol,
                           field_columns=field_columns,
                           tags=tags,
                           time_precision='ms',
                           numeric_precision='full',
                           batch_size=50000)


def load_multiple_files(db_client, dir_path):
    """

    Args:
        db_client:
        dir_path:

    Returns:

    """
    dir_path = pathlib.Path(dir_path)
    files = dir_path.glob('**/*.gz')

    for each_file in files:
        init_preparing_time = time.time()

        print('Working on: {}'.format(each_file))
        df = prepare_data_for_securities_master(each_file)

        end_preparing_time = time.time()
        delta_preparing = end_preparing_time - init_preparing_time
        time_str = time.strftime("%H:%M:%S", time.gmtime(delta_preparing))
        print('Data ready to load')
        print('Time processing file: {}'.format(time_str))

        symbol = each_file.parts[-1][0:6]
        write_to_db(db_client, df, symbol)
        end_loading_time = time.time()
        delta_loading = end_loading_time - end_preparing_time
        time_str = time.strftime("%H:%M:%S", time.gmtime(delta_loading))
        print('Data loaded to databases')
        print('Time loading to db: {}'.format(time_str))
        print('##############################################################')


if __name__ == '__main__':
    start_time = time.time()

    my_client = influx_client(client_type='dataframe')

    my_dir = "/media/sf_Trading/data/clean_fxcm"
    load_multiple_files(my_client, my_dir)


    # gen = query_to_db().get_points()
    # for x in gen:
    #    print(x)

    #my_path = "/media/sf_Trading/data/example_data/small.csv.gz"
    #x = prepare_data_for_securities_master(my_path)
    #print(x.head(10))
    #write_to_db(my_client, x, 'eurusd')
    #ans = query_to_db(my_client)
    #print(ans)

    x = time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))
    print("Running for : {} ".format(x))
