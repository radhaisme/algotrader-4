# coding=utf-8
"""
Converts ticks to bars
"""
import datetime
import logging

import pandas as pd
from influxdb.exceptions import InfluxDBServerError, InfluxDBClientError

from databases.fxcm_tick_insert import series_by_filename
from databases.influx_manager import influx_client
from log.logging import setup_logging


def time_bounds(measurement, symbol, provider, position=('FIRST', 'LAST')):
    """ Get the first and/or last datetime for a series in a measurement

    :param measurement:
    :param symbol:
    :param provider:
    :param position:
    :return:
    """
    client = influx_client(client_type='client', user_type='reader')
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
            raise SystemError

        ans[each_position] = one_minute_adjustment(time_on_db)
        ans['symbol'] = symbol
        ans['provider'] = provider
        ans['measurement'] = measurement

    client.close()

    return ans


def one_minute_adjustment(datetime_to_adjust):
    """ Adjust a datetime to the start of the minute
    Basically removes seconds and milliseconds.
    :param datetime_to_adjust:
    :return: adjusted datetime object
    """
    ans = datetime_to_adjust.replace(minute=0)
    ans = ans.replace(second=0)
    # remove milliseconds as string, replace works with microseconds no
    # milliseconds
    ans = ans.strftime("%Y-%m-%d %H:%M:%S")
    return pd.to_datetime(ans)


def ticks_to_bars(ticks, frequency='1min'):
    """Re sample ticks [timestamp bid, ask) to bars OHLC of selected frequency
    https://stackoverflow.com/a/17001474/3512107
    :param ticks:
    :param frequency:
    https://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
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
    """
    ticks['mid'] = ticks.mean(axis=1)
    ticks.drop(['bid', 'ask'], axis=1, inplace=True)

    bars = ticks.resample(rule=frequency, level=0).ohlc()

    # Drop N/A. When there are no tick, do not create a bar
    bars.dropna(inplace=True)

    # Drop multi-index, Influx write has problem with that
    bars.columns = bars.columns.droplevel(0)

    return bars


def insert_bars_to_sec_master(client, bars, table_prefix, frequency, symbol,
                              provider):
    """Performs data insertion of bar data to securities master database

    :param client: influx database client object
    :param bars: DF with the OHLC data
    :param table_prefix: str to name the output table
    :param frequency: bars frequency
    :param symbol: symbol identification
    :param provider: data provider identification
    :return: None
    """
    tags = {'provider': provider,
            'symbol': symbol,
            'frequency': frequency}
    field_columns = ['open', 'high', 'low', 'close']
    protocol = 'json'

    # construct table (measurement) name as give prefix + frequency indicator
    table_name = '{}_{}'.format(table_prefix, frequency)

    try:
        # Insert in database
        client.write_points(dataframe=bars,
                            measurement=table_name,
                            tags=tags,
                            time_precision='ms',
                            protocol=protocol,
                            numeric_precision='full',
                            field_columns=field_columns)
    except (InfluxDBClientError, InfluxDBServerError):
        logger.exception('Error inserting data for %s at &s', tags, table_name)
        raise SystemError


def tick_resampling(symbol, provider, input_table, output_table_prefix,
                    custom_dates=False, start_time=None, end_time=None,
                    frequency='1min'):
    """Re sample tick data in securities master to desired frequency

    :param symbol:
    :param provider:
    :param input_table:
    :param output_table_prefix:
    :param custom_dates:
    :param start_time:
    :param end_time:
    :param frequency:
    :return:
    """

    # Define the bounds
    if custom_dates:
        # We use the given dates as bounds
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)
    else:
        # We find the bounds of the series in the database
        bounds = time_bounds(measurement=input_table, symbol=symbol,
                             provider=provider)
        start_time = bounds['FIRST']
        end_time = bounds['LAST']

    client = influx_client(client_type='dataframe', user_type='writer')

    # Define the time extension of each query.
    # The bigger the number, more RAM needed.
    delta = datetime.timedelta(hours=24)

    while start_time <= end_time:
        partial_end = start_time + delta
        cql = 'SELECT time, bid, ask FROM {} ' \
              'WHERE symbol=\'{}\' ' \
              'AND provider=\'{}\' ' \
              'AND time>=\'{}\' ' \
              'AND time<\'{}\''.format(input_table, symbol, provider,
                                       start_time, partial_end)

        try:
            # Get the ticks requested
            ticks = client.query(cql)[input_table]
            # Call the re sampling function
            bars = ticks_to_bars(ticks)
            # Insert into securities master database
            insert_bars_to_sec_master(client, bars, output_table_prefix,
                                      frequency, symbol, provider)
            logger.info('Data for %s from %s to %s OK!', symbol,
                        start_time, end_time )
        except (KeyError, InfluxDBClientError):
            logger.warning('No data for %s from %s to %s', symbol,
                           start_time, end_time)
        start_time = partial_end
    client.close()


def get_other_tags(table, filename):
    """

    :param table:
    :param filename:
    :return:
    """
    try:
        # get tags
        cql = 'SELECT * ' \
              'FROM {} ' \
              'WHERE filename=\'{}\' ' \
              'LIMIT 1'.format(table,
                               filename)
        client = influx_client(client_type='client', user_type='reader')
        response = next(client.query(cql).get_points())
    except (InfluxDBClientError, InfluxDBServerError):
        logger.exception('Could no query the database')
        raise SystemError

    return {'provider': response['provider'],
            'symbol': response['symbol']}


def load_all_series(input_table='fx_ticks'):
    """ Load all series of a table and call resampling

    :param input_table:
    :return:
    """

    # get the file names already in database
    tick_series = series_by_filename(tag='filename', clean_store_dirpath='')

    # now we need to get the other tags
    # and do the resampling to the desired frequency
    for each_series in tick_series.keys():
        tags = get_other_tags(input_table, each_series)

        # TODO: Check if already series exist, and until what date
        # if series_exist:
        #     custom dates

        tick_resampling(output_table_prefix='fx',
                        symbol=tags['symbol'],
                        provider=tags['provider'],
                        input_table=input_table,
                        custom_dates=False,
                        frequency='1min' )
    logger.info('Insert all series finished.')


def main():
    logger.info('############ NEW RUN ##################')
    # load_all_series()
    tick_resampling(symbol='AUDCAD',
                    provider='fxcm',
                    input_table='fx_ticks',
                    custom_dates=False,
                    output_table_prefix='fx')


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('tick_resampling')
    main()
