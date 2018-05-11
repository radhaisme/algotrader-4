# coding=utf-8
import sys
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
    for each_position in position:
        cql = 'SELECT {}(ask) ' \
              'FROM \"{}\" ' \
              'WHERE symbol=\'{}\' ' \
              'AND provider=\'{}\''.format(each_position,
                                           measurement,
                                           symbol,
                                           provider)

        try:
            time_on_db = pd.to_datetime(next(client.query(query=cql)[measurement])['time'])
        except (InfluxDBClientError, InfluxDBServerError):
            logger.exception('Error reading time bounds')
            sys.exit(-1)

        ans[each_position] = one_minute_adjustment(time_on_db)
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


def load_all_series(input_table='fx_ticks'):
    """
    Load all series of a table and call resampling
    Args:
        input_table: ticks table [timestamp, bid, ask]

    Returns:

    """
    t0 = datetime.datetime.now()

    tick_series = available_series(input_table)
    for each_series in tick_series:
        t1 = datetime.datetime.now()

        tick_resampling(symbol=each_series[0], provider=each_series[1], input_table=input_table)

        t2 = datetime.datetime.now()
        logger.info('TOTAL RUNNING TIME FOR {} - {}'.format(each_series[0], t2 - t1))

    t4 = datetime.datetime.now()
    logger.info('TOTAL RUNNING TIME - {}'.format(t4 - t0))


def tick_resampling(symbol, provider, input_table, custom_dates=False,
                    start_time=None, end_time=None, frequency='1min'):
    """
    Re-sample 24hr of tick data for a symbol-provider loading to securities master
    Args:
        symbol:
        provider:
        input_table:
        custom_dates:
        start_time:
        end_time:
        frequency:

    Returns:

    """

    # Define the bounds
    if custom_dates:
        # We use the given dates as bounds
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)
    else:
        # We find the bounds of the series in the database
        bounds = time_bounds(measurement=input_table, symbol=symbol, provider=provider)
        start_time = bounds['FIRST']
        end_time = bounds['LAST']

    client = influx_client(client_type='dataframe')

    # Define the time extension of each query.
    # The bigger the number, more RAM needed.
    delta = datetime.timedelta(hours=24)

    while start_time <= end_time:
        logger.info('Working on {} at {}'.format(symbol, start_time))
        partial_end = start_time + delta

        cql = 'SELECT time, bid, ask FROM {} ' \
              'WHERE symbol=\'{}\' ' \
              'AND provider=\'{}\' ' \
              'AND time>=\'{}\' ' \
              'AND time<\'{}\''.format(input_table, symbol, provider, start_time, partial_end)

        try:
            # Get the ticks requested
            ticks = client.query(cql)[input_table]
        except KeyError:
            logger.warning('No data for {} at {}'.format(symbol, start_time))
            break

        # Call the re sampling function
        bars = ticks_to_bars(ticks)

        # Insert into securities master database
        insert_bars_to_sec_master(client, bars, frequency, symbol, provider)

        start_time = partial_end

    client.close()


def ticks_to_bars(ticks, frequency='1min'):
    """
    Re sample ticks [timestamp bid, ask) to bars OHLC of selected frequency
    https://stackoverflow.com/a/17001474/3512107

    Args:
        ticks:
        frequency: https://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
                    Alias	Description
                    B	business day frequency
                    C	custom business day frequency
                    D	calendar day frequency
                    W	weekly frequency
                    M	month end frequency
                    SM	semi-month end frequency (15th and end of month)
                    BM	business month end frequency
                    CBM	custom business month end frequency
                    MS	month start frequency
                    SMS	semi-month start frequency (1st and 15th)
                    BMS	business month start frequency
                    CBMS	custom business month start frequency
                    Q	quarter end frequency
                    BQ	business quarter end frequency
                    QS	quarter start frequency
                    BQS	business quarter start frequency
                    A, Y	year end frequency
                    BA, BY	business year end frequency
                    AS, YS	year start frequency
                    BAS, BYS	business year start frequency
                    BH	business hour frequency
                    H	hourly frequency
                    T, min	minutely frequency
                    S	secondly frequency
                    L, ms	milliseconds
                    U, us	microseconds
                    N	nanoseconds

    Returns: DF

    """
    ticks['mid'] = ticks.mean(axis=1)
    ticks.drop(['bid', 'ask'], axis=1, inplace=True)

    bars = ticks.resample(rule=frequency, level=0).ohlc()

    # Drop N/A. When there are no tick, do not create a bar
    bars.dropna(inplace=True)

    # Drop multi-index, Influx write has problem with that
    bars.columns = bars.columns.droplevel(0)

    return bars


def insert_bars_to_sec_master(client, bars, frequency, symbol, provider):
    """

    Args:
        client:
        bars:
        frequency:
        symbol:
        provider:

    Returns:

    """
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
    except (InfluxDBClientError, InfluxDBServerError):
        logger.exception('Error inserting data for {} at {}'.format(tags, measurement))
        sys.exit(-1)


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)

    sdate = '2016-09-01 00:00:00.000'
    edate = '2016-10-01 00:00:00.000'
    logger.info('############ NEW RUN ##################')
    tick_resampling(symbol='EURUSD',
                    provider='fxcm',
                    input_table='fx_tick',
                    custom_dates=True,
                    start_time=sdate,
                    end_time=edate)




