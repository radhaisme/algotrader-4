# -*- coding: utf-8 -*-

import logging
import sys

from log.logging import setup_logging
from influxdb import DataFrameClient
from influxdb import InfluxDBClient
from influxdb.exceptions import *
from pprint import pprint

from common.config import influx_config


def influx_client(client_type='client'):
    """Instantiate a connection to the InfluxDB.

    :param client_type:
    :return:
    """
    # Get the configuration info
    config = influx_config()

    # initialize the client
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    dbname = config['database']

    try:
        if client_type == 'client':
            client = InfluxDBClient(host, port, user, password, dbname)
            logging.info('Logged to {} influx as client with user \'{}\''.format(dbname, user))
            return client
        elif client_type == 'dataframe':
            client = DataFrameClient(host, port, user, password, dbname)
            logging.info('Logged to {} influx as dataframe client with user \'{}\''.format(dbname, user))
            return client
    except (InfluxDBServerError, InfluxDBClientError):
        logging.exception('Can not connect to database Influxdb.')
        sys.exit(-1)


def available_series(measurement):
    """Return list of tuples (symbol, provider) with available series in a measurement

    :param measurement:
    :return:
    """
    cql = "SELECT LAST(bid), symbol, provider " \
          "FROM \"{}\" " \
          "GROUP BY symbol".format(measurement)
    try:
        ans = influx_client().query(query=cql)
        return [(x[0].get('symbol'), x[0].get('provider')) for x in ans]
    except (InfluxDBClientError, InfluxDBClient):
        logging.exception('Can not obtain series info.')
        sys.exit(-1)


def db_server_info():
    """Print out info about the Influx database

    """

    client = influx_client()

    dbs = client.get_list_database()
    usr = client.get_list_users()
    msr = client.get_list_measurements()
    ret = client.get_list_retention_policies()

    print('###########################################')
    print('#                                         #')
    print('#       INFLUX DATABASE SERVER INFO       #')
    print('#                                         #')
    print('###########################################')
    print('\n')
    print('################ DATABASES ################\n')
    for db in dbs:
        db_ret = client.get_list_retention_policies(db['name'])
        print('Database: \"{}\" with policy: \"{}\"'.format(db['name'], db_ret[0]['name']))

    print('\n################   USERS   ################\n')
    for us in usr:
        print('User \"{}\" is admin: \"{}\"'.format(us['user'], us['admin']))
    print('\n########### RETENTION POLICIES ############\n')
    for r in ret:
        print(r)
    print('\n############## MEASUREMENTS ###############\n')
    for m in msr:
        print(m)

    client.close()


def series_info_count(measurement, series):
    """Return count information about one series in a measurement.

    :param measurement:
    :param series:
    :return:
    """

    cql = "SELECT COUNT(*) " \
          "FROM \"{}\" " \
          "WHERE symbol=\'{}\' AND " \
          "provider=\'{}\'".format(measurement, series[0], series[1])

    try:
        logging.info("Querying {} at {}".format(series, measurement))
        client = influx_client()
        response = client.query(query=cql)
        client.close()
    except (InfluxDBClientError, InfluxDBClientError):
        logging.exception('Can not obtain series info.')
        sys.exit(-1)

    datapoint = next(response.get_points())
    del datapoint['time']

    return datapoint


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)

    tables = ['fx_tick', 'fx_ticks']
    series = [('AUDCAD', 'fxcm'), ('AUDCHF', 'fxcm'), ('AUDJPY', 'fxcm')]

    ans = {}
    for each_t in tables:
        for each_ser in series:
            my_k = each_t + '_' + each_ser[0]
            ans[my_k] = series_info_count(measurement=each_t, series=each_ser)

    pprint(ans)

