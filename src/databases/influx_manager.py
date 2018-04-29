# -*- coding: utf-8 -*-

from pprint import pprint
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




if __name__ == '__main__':
    pprint(available_series())
