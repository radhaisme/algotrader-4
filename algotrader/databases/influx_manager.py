# -*- coding: utf-8 -*-
"""
Manage the connections to Influx Server
"""
import logging
import sys

from influxdb import DataFrameClient
from influxdb import InfluxDBClient
from influxdb.exceptions import *

from common.settings import ATSett
from log.logging import setup_logging


def influx_client(client_type='client', user_type='reader'):
    """Instantiate a connection to the InfluxDB.

    :param client_type: 'client' / 'dataframe'
    :param user_type: 'reader' / 'writer' / 'admin'
    :return:
    """
    # Get the configuration info
    config = ATSett().influx_config()

    # initialize the client
    host = config['host']
    port = config['port']
    user = config['user_' + user_type]
    password = config['password_' + user_type]
    dbname = config['database']

    try:
        if client_type == 'client':
            client = InfluxDBClient(host, port, user, password, dbname)
            return client
        elif client_type == 'dataframe':
            client = DataFrameClient(host, port, user, password, dbname)
            return client
    except (InfluxDBServerError, InfluxDBClientError, KeyError):
        logging.exception('Can not connect to database Influxdb.')
        raise SystemError


def available_series(measurement):
    """Return list of tuples (symbol, provider) with available
     series in a measurement

    :param measurement:
    :return:
    """
    cql = "SELECT count(*) FROM \"{}\" " \
          "GROUP BY filename LIMIT 1".format(measurement)
    try:
        c = influx_client(client_type='dataframe', user_type='reader')
        ans = c.query(query=cql)[measurement]
        return [(x[0].get('symbol'),
                 x[0].get('filename'),
                 x[0].get('provider')) for x in ans]
    except (InfluxDBClientError, InfluxDBClient):
        logging.exception('Can not obtain series info.')
        sys.exit(-1)


def db_server_info():
    """Print out info about the Influx database

    """

    client = influx_client(user_type='admin')

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
        print('Database: \"{}\" '
              'with policy: \"{}\"'.format(db['name'],
                                           db_ret[0]['name']))

    print('\n################   USERS   ################\n')
    for us in usr:
        print('User \"{}\" '
              'is admin: \"{}\"'.format(us['user'],
                                        us['admin']))
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
          "provider=\'{}\'".format(measurement,
                                   series[0],
                                   series[1])

    try:
        logging.info("Querying {} at {}".format(series, measurement))
        client = influx_client(user_type='reader')
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
    # db_server_info()
    series = available_series('fx_ticks')

    for s in series:
        print(s)
