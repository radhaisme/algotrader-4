# -*- coding: utf-8 -*-

import csv
import json
import pandas as pd
import datetime
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from common.config import influx_config

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

    df['last_update'] = datetime.datetime.now()

    return df


def connect_to_influx():
    """
    Instantiate a connection to the InfluxDB.

    """

    config = influx_config()
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    dbname = config['database']

    return InfluxDBClient(host, port, user, password, dbname)

def csv_to_json_influx(my_file):
    f = open(my_file, 'rb')
    reader = csv.DictReader(f)

    jsonoutput = 'output.json'
    with open(jsonoutput, 'a') as f:
        for x in reader:
            json.dump(x, f)
            f.write('\n')






def main():
    my_path = "/media/sf_D_DRIVE/Trading/data/example_data/small.csv.gz"
    x = prepare_data_for_securities_master(my_path)
    print(x)



if __name__ == '__main__':
    connect_to_influx()
    #main()