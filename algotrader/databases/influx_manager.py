# -*- coding: utf-8 -*-
"""
Manage the connections to Influx Server
"""
import logging


from influxdb import DataFrameClient
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError, InfluxDBClientError

from common.settings import ATSett


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

    client = None
    try:
        if client_type == 'client':
            client = InfluxDBClient(host, port, user, password, dbname)
        elif client_type == 'dataframe':
            client = DataFrameClient(host, port, user, password, dbname)

    except (InfluxDBServerError, InfluxDBClientError, KeyError):
        logging.exception('Can not connect to database Influxdb.')
        raise SystemError
    return client


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


def influx_qry(cql, client_type='client', user_type='reader'):
    """Generic database query

    """
    try:
        client = influx_client(client_type=client_type, user_type=user_type)
        response = client.query(cql)
        client.close()
    except (InfluxDBServerError, InfluxDBClientError):
        logging.exception('Can not query the database')
        raise SystemError

    return response


def influx_writer(data, field_columns, tags, into_table):
    """Generic database writer

    """
    logging.info('Insert {} into table \'{}\''.format(tags.values(),
                                                      into_table))
    protocol = 'json'
    try:
        client = influx_client(client_type='dataframe', user_type='writer')
        client.write_points(dataframe=data,
                            measurement=into_table,
                            protocol=protocol,
                            field_columns=field_columns,
                            tags=tags,
                            time_precision='u',
                            numeric_precision='full',
                            batch_size=10000)
        client.close()
        logging.info('Data insert OK!')
    except (InfluxDBServerError, InfluxDBClientError):
        logging.exception('Error data insert - {}'.format(tags.values()))
        raise SystemError


def delete_series(tags):
    """Deletes series in current database

    """

    try:
        client = influx_client(client_type='client', user_type='writer')
        client.delete_series(tags=tags)
        client.close()
    except (InfluxDBClientError, InfluxDBServerError):
        logging.exception('Could not delete series {}'.format(tags))
