from databases.influx_manager import influx_client
from influxdb.exceptions import *
import logging
from common.utilities import fn_timer
from common.config import store_clean_fxcm
import pathlib


@fn_timer
def series_in_table(table):

    cql = 'SELECT COUNT(bid) FROM {} GROUP BY filename'.format(table)
    try:
        client = influx_client(client_type='dataframe', user_type='reader')
        response = client.query(cql).items()
        client.close()
    except InfluxDBClientError:
        SystemError

    ans = dict()
    for v in response:
        filename = v[0][1][0][1]
        row_count = v[1]['count'].iloc[0]
        store_path = store_path_constructor(filename=filename)
        ans[filename] = {'row_count': row_count,
                         'filepath': store_path}

    return ans


def store_path_constructor(filename):
    store = pathlib.Path(store_clean_fxcm())
    symbol = filename[:6]
    yr = filename[7:11]
    full_filename = filename + '.csv.gz'

    return store / symbol / yr / full_filename


if __name__ == '__main__':

    series = series_in_table(table='fx_ticks')

    for k, v in series.items():
        print(k, v['row_count'], v['filepath'])




