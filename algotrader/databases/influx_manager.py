# -*- coding: utf-8 -*-

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

    try:
        if client_type == 'client':
            return InfluxDBClient(host, port, user, password, dbname)
        elif client_type == 'dataframe':
            return DataFrameClient(host, port, user, password, dbname)
    except Exception as e:
        print(e)
        return False


def available_series(measurement='fx_tick'):
    """
    List of tuples (symbol, provider) with available series in a measurement
    Args:
        measurement: "table" to query
        group_by: tag to group by

    Returns: list of available series within the measurement

    """

    cql = "SELECT LAST(bid), symbol, provider " \
          "FROM \"{}\" " \
          "GROUP BY symbol".format(measurement)

    ans = influx_client().query(query=cql)
    return [(x[0].get('symbol'), x[0].get('provider')) for x in ans]


def db_server_info():
    """

    Returns: Print out info about the database

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


