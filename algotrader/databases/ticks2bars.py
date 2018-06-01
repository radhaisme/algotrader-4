# coding=utf-8
"""
Converts ticks to bars
"""
import datetime
import logging
from common.utilities import iter_islast
import pandas as pd
from influxdb.exceptions import InfluxDBServerError, InfluxDBClientError

from databases.influx_manager import influx_qry
from log.log_settings import setup_logging


def time_bounds(table, tags, position=('FIRST', 'LAST')):
    """ Get the first and/or last datetime for a series in a table
    a Series is defined by its tags
    """
    # construct WHERE part of the cql query, using all tags provided
    where_cql = 'WHERE '
    for k, islast in iter_islast(tags):
        constructor = '\"' + k + '\"=' + '\'' + tags[k] + '\' '
        if islast:
            where_cql = where_cql + constructor
        else:
            where_cql = where_cql + constructor + 'AND '

    ans = {}
    for each_position in position:
        cql = 'SELECT {}(open) ' \
              'FROM \"{}\" {}'.format(each_position,
                                      table,
                                      where_cql)

        response = influx_qry(cql).get_points()
        time_on_db = pd.to_datetime([t for t in response][0]['time'])

        # adjust to lower minute
        # construct the answer as a dict
        ans[each_position] = one_minute_adjustment(time_on_db)

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


def tick_resampling(input_table, output_table_prefix, tags, custom_dates=False,
                    start_datetime=None, end_datetime=None, ):
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
        start_time = pd.to_datetime(start_datetime)
        end_time = pd.to_datetime(end_datetime)
    else:
        # We find the bounds of the series in the database fx-tick
        bounds = time_bounds(table=input_table, symbol=symbol,
                             provider=provider)
        start_time = bounds['FIRST']
        end_time = bounds['LAST']


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
            ticks = influx_qry(cql)[input_table]
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


# def get_series_tags(filename):
#     """
#
#     :param filename:
#     :return:
#     """
#     try:
#         # get tags
#         cql = 'SHOW TAG VALUES ON "securities_master" ' \
#               'WITH KEY IN ("provider", "symbol", "filename")' \
#               ' WHERE filename=\'{}\''.format(filename)
#
#         client = influx_client(client_type='client', user_type='reader')
#         response = client.query(cql).get_points()
#         client.close()
#     except (InfluxDBClientError, InfluxDBServerError):
#         logger.exception('Could no query the database')
#         raise SystemError
#
#     return {each_dict['key']: each_dict['value'] for each_dict in response}
#


def get_series_tags(table):
    """Returns tags of each series in a table

    :param table:
    :return: list of dictionaries
    """
    # get tags
    cql = 'SHOW SERIES ON "securities_master" FROM \"{}\"'.format(table)
    response = influx_qry(cql).get_points()

    # Create a list from the generator
    # Everything before the first comma is the measurement name - remove
    all_series = [series['key'][len(table)+1:] for series in response]

    # split the tags, comma separated
    series_split = []
    for each_series in all_series:
        series_split.append([x.strip() for x in each_series.split(',')])

    # the value before the = is the tag key
    # the value after the = is the tag value
    ans = []
    for each_split in series_split:
        inner_dict = dict()
        for each_part in each_split:
            series_tags = each_part.split('=')
            # Creates a dictionary for each series
            inner_dict[series_tags[0]] = series_tags[1]
        # store all dictionaries in a list
        ans.append(inner_dict)

    return ans


def load_all_series(input_table, output_table_prefix, freq):
    """ Load all series of a table and call resampling

    """
    output_table = output_table_prefix + freq

    tick_series = get_series_tags(input_table)
    bar_series = get_series_tags(output_table)

    for each_series in tick_series:
        tags_in_bar_series = {'frequency': freq,
                              'symbol': each_series['symbol'],
                              'provider': each_series['provider']}

        # Verify if series already in database
        if tags_in_bar_series in bar_series:
            # get the dates
            bounds = time_bounds(output_table, tags_in_bar_series)
            custom_dates = True
            start_datetime = bounds['LAST']
        else:
            custom_dates = False
            start_datetime = None

        tick_resampling(output_table_prefix='fx',
                        tags=tags_in_bar_series,
                        input_table=input_table,
                        custom_dates=custom_dates,
                        start_datetime=start_datetime)


    logger.info('Insert all series finished.')


def main():
    logger.info('############ NEW RUN ##################')

    load_all_series(input_table='fx_ticks', output_table_prefix='fx_',
                    freq='1min')
    # tick_resampling(symbol='AUDCAD',
    #                 provider='fxcm',
    #                 input_table='fx_ticks',
    #                 custom_dates=False,
    #                 output_table_prefix='fx')


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('tick_resampling')
    main()
