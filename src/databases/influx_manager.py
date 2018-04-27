

from common.config import influx_config
from influxdb import DataFrameClient
from influxdb import InfluxDBClient


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


def rows_per_serie():
    table = '"fx_tick"'
    symbol = "'AUDCAD'"
    q = "SELECT COUNT(bid) FROM " + table +' WHERE "symbol" = ' + symbol

    print(q)
    ans = influx_client().query(query=q)
    print(ans)


if __name__ == '__main__':

    rows_per_serie()