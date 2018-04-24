# -*- coding: utf-8 -*-

import pathlib

import time
import pandas as pd
from influxdb import DataFrameClient
from influxdb import InfluxDBClient

from common.config import influx_config


def influx_client(client_type='client'):
    """
    Instantiate a connection to the InfluxDB.

    Returns: influx client

    """
    # Get the configuration info
    config = influx_config()

    # initialize the client
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    dbname = config['database']

    if client_type == 'client':
        return InfluxDBClient(host, port, user, password, dbname)
    elif client_type == 'dataframe':
        return DataFrameClient(host, port, user, password, dbname)


def prepare_data_for_securities_master(file_path):
    """
    Opens a file downloaded from FXMC and format it to upload to Securities Master database
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
                     date_parser=pd.to_datetime,
                     index_col=[0])

    #df['last_update'] = datetime.datetime.now()

    return df


def write_to_db():
    my_path = "/media/sf_D_DRIVE/Trading/data/example_data/AUDCAD"
    client = influx_client(client_type='dataframe')
    my_path = pathlib.Path(my_path)
    files = my_path.glob('**/*.gz')

    for each_file in files:
        init_preparing_time = time.time()
        print('Working on: {}'.format(each_file))
        df = prepare_data_for_securities_master(each_file)

        end_preparing_time = time.time()
        delta_preparing = end_preparing_time - init_preparing_time
        time_str = time.strftime("%H:%M:%S", time.gmtime(delta_preparing))
        print('Data ready to load')
        print('Time processing file: {}'.format(time_str))

        protocol = 'json'
        field_columns = ['bid', 'ask']
        tags = {'symbol': 'AUDCAD',
                'provider': 'fxcm'}
        table = 'fx_tick_data'

        client.write_points(dataframe=df,
                            measurement=table,
                            protocol=protocol,
                            field_columns=field_columns,
                            tags=tags,
                            time_precision='ms',
                            numeric_precision=None,
                            batch_size=50000)

        end_loading_time = time.time()
        delta_loading = end_loading_time - end_preparing_time
        time_str = time.strftime("%H:%M:%S", time.gmtime(delta_loading))
        print('Data loaded to database')
        print('Time loading to db: {}'.format(time_str))
        print('##############################################################')


def query_to_db():
    query = "SELECT *::field from " + '"fx_tick_data" WHERE "provider" = ' + "'fxcm'"
    print(query)
    client = influx_client(client_type='client')

    ans = client.query(query=query, epoch=None)
    return ans


if __name__ == '__main__':

    write_to_db()


    #gen = query_to_db().get_points()
    #for x in gen:
    #    print(x)
