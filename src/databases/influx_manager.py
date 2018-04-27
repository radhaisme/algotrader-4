# -*- coding: utf-8 -*-

from common.config import influx_config
from influxdb import DataFrameClient
from influxdb import InfluxDBClient
from pprint import pprint


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


def available_series(measurement='fx_tick', group_by='symbol'):
    """

    Args:
        measurement: "table" to query
        group_by: tag to group by

    Returns: list of available series within the measurement

    """

    cql = "SELECT COUNT(bid) FROM \"{}\" GROUP BY {}".format(measurement, group_by)

    print(cql)
    ans = influx_client().query(query=cql)

    pprint(ans)



if __name__ == '__main__':
    available_series()
