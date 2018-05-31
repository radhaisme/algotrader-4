# -*- coding: utf-8 -*-
"""
Manage the connections to Influx Server
"""
import logging
import sys

from influxdb import DataFrameClient
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError, InfluxDBClientError

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
          "GROUP BY filename".format(measurement)
    ans = dict()
    try:
        client = influx_client(client_type='dataframe', user_type='reader')
        qry_ans = client.query(query=cql)[measurement]

        # return [(x[0].get('symbol'),
        #          x[0].get('filename'),
        #          x[0].get('provider')) for x in qry_ans]
    except (InfluxDBClientError, InfluxDBServerError):
        logging.exception('Can not obtain series info.')
        raise SystemError

    # for item in qry_ans:
    #     key = item[0].get('filename')
    #     value = {'symbol':(x[0].get('symbol'),
    #              'provider'
    #                        }



def db_server_info():
    """Print out info about the Influx database

    """

    client = influx_client(user_type='admin')

    databases_lst = client.get_list_database()
    users_lst = client.get_list_users()
    table_lst = client.get_list_measurements()
    policy_lst = client.get_list_retention_policies()

    print('###########################################')
    print('#                                         #')
    print('#       INFLUX DATABASE SERVER INFO       #')
    print('#                                         #')
    print('###########################################')
    print('\n')
    print('################ DATABASES ################\n')
    for database in databases_lst:
        db_ret = client.get_list_retention_policies(database['name'])
        print('Database: \"{}\" '
              'with policy: \"{}\"'.format(database['name'],
                                           db_ret[0]['name']))

    print('\n################   USERS   ################\n')
    for each_user in users_lst:
        print('User \"{}\" '
              'is admin: \"{}\"'.format(each_user['user'],
                                        each_user['admin']))
    print('\n########### RETENTION POLICIES ############\n')
    for each_policy in policy_lst:
        print(each_policy)
    print('\n############## MEASUREMENTS ###############\n')
    for each_table in table_lst:
        print(each_table)

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
        logging.info("Querying %s at %s", series, measurement)
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
    my_series = available_series('fx_ticks')

    for s in my_series:
        print(s)
