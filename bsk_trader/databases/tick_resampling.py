# coding=utf-8

import datetime
import pandas as pd
from databases.influx_manager import available_series
from databases.influx_manager import influx_client
from influxdb.exceptions import *
from log.logging import setup_logging

import logging


def time_bounds(measurement, symbol, provider, position=['FIRST', 'LAST']):
    """
    Get the first and/or last datetime for a series in a measurement
    Args:
        measurement: table
        symbol: tag
        provider: tag
        position: list

    Returns: dict

    """
    client = influx_client()
    ans = {}
    for each_posicion in position:
        cql = 'SELECT {}(ask) ' \
              'FROM \"{}\" ' \
              'WHERE symbol=\'{}\' ' \
              'AND provider=\'{}\''.format(each_posicion,
                                           measurement,
                                           symbol,
                                           provider)

        time_on_db = pd.to_datetime(next(client.query(query=cql)[measurement])['time'])
        ans[each_posicion] = one_minute_adjustment(time_on_db)
        ans['symbol'] = symbol
        ans['provider'] = provider
        ans['measurement'] = measurement
    client.close()
    return ans


def one_minute_adjustment(datetime_to_adjust):
    """

    Args:
        datetime_to_adjust:

    Returns: datetime adjustment to the previous minute

    """
    ans = datetime_to_adjust.replace(minute=0)
    ans = ans.replace(second=0)
    # remove milliseconds as string, replace works with microseconds
    ans = ans.strftime("%Y-%m-%d %H:%M:%S")
    return pd.to_datetime(ans)


def tick_resampling(symbol, provider, input_table, custom_dates=False,
                    start_time=None, end_time=None, frequency='1m'):

    if custom_dates:
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)
    else:
        bounds = time_bounds(measurement=input_table, symbol=symbol, provider=provider)
        start_time = bounds['FIRST']
        end_time = bounds['LAST']

    client = influx_client(client_type='dataframe')
    delta = datetime.timedelta(hours=24)

    while start_time < end_time:
        logger.info('Working on {} at {}'.format(symbol, start_time))

        partial_end = start_time + delta
        cql = 'SELECT time, bid, ask FROM {} ' \
              'WHERE symbol=\'{}\' ' \
              'AND provider=\'{}\' ' \
              'AND time>=\'{}\' ' \
              'AND time<\'{}\''.format(input_table, symbol, provider, start_time, partial_end)

        try:
            ticks = client.query(cql)[input_table]
            bars = ticks_to_bars(ticks)
            insert_bars_to_sec_master(client, bars, frequency, symbol, provider)
        except KeyError:
            logger.info('No data for {} at {}'.format(symbol, start_time))

        start_time = partial_end

    client.close()


def ticks_to_bars(ticks, frequency='1m'):

    ticks['mid'] = ticks.mean(axis=1)
    ticks.drop(['bid', 'ask'], axis=1, inplace=True)

    bars = ticks.resample(rule=frequency, level=0).ohlc()

    # Drop multiindex, Influx write has problem with that
    bars.columns = bars.columns.droplevel(0)

    # Drop N/A. When there are no tick, do not create a bar
    bars.dropna(inplace=True)

    return bars


def insert_bars_to_sec_master(client, bars, frequency, symbol, provider):

    tags = {'provider': provider,
            'symbol': symbol}
    field_columns = ['open', 'high', 'low', 'close']
    protocol = 'json'
    measurement = 'fx_{}'.format(frequency)

    try:
        # Insert in database
        client.write_points(dataframe=bars,
                            measurement=measurement,
                            tags=tags,
                            time_precision='ms',
                            protocol=protocol,
                            numeric_precision='full',
                            field_columns=field_columns)
        logger.info('Insert successful for {} at {}'.format(symbol, measurement))
        return True
    except (InfluxDBClientError, InfluxDBServerError):
        logger.exception('Error inserting data for {} at {}'.format(tags, measurement))
        return False


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)

    sdate = '2018-02-01 00:00:00.000'
    edate = '2018-02-02 00:00:00.000'

    t0 = datetime.datetime.now()

    table = 'fx_tick'
    tick_series = available_series(table)

    for each_serie in tick_series:
        t1 = datetime.datetime.now()

        prov = each_serie[1]
        sym = each_serie[0]
        tick_resampling(symbol=sym, provider=prov, input_table=table)

        t2 = datetime.datetime.now()
        logger.info('TOTAL RUNNING TIME FOR {} - {}'.format(sym, t2-t1))

    t4 = datetime.datetime.now()
    logger.info('TOTAL RUNNING TIME FOR ALL - {}'.format(sym, t4 - t0))
