# coding=utf-8
"""
Converts ticks to bars
"""
import datetime
import logging
import itertools

import pandas as pd
from influxdb.exceptions import InfluxDBServerError, InfluxDBClientError

import databases.influx_manager as db_man
from common.utilities import iter_islast
from log.log_settings import setup_logging, log_title


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


def get_field_keys(table):
    """ Field keys for a selected table

    :param table:
    :return: list op dictionaries
    """
    cql = 'SHOW FIELD KEYS FROM \"{}\"'.format(table)
    response = db_man.influx_qry(cql).get_points()

    return [x for x in response]


def time_bounds(table, tags, position=('FIRST', 'LAST')):
    """ Get the first and/or last datetime for a series in a table
    a Series is defined by its tags
    """

    # We need one field key to make the query and get the time, for select *
    # returns UNIX time.
    field_keys = get_field_keys(table)
    field_key_to_qry = field_keys[0]['fieldKey']

    # construct WHERE part of the cql query, using all tags provided
    where_cql = 'WHERE '
    for k, islast in iter_islast(tags):
        constructor = '\"' + k + '\"=' + '\'' + tags[k] + '\' '
        where_cql = where_cql + constructor
        if not islast:
            where_cql += 'AND '

    ans = {}
    for each_position in position:
        cql = 'SELECT {}({}) ' \
              'FROM \"{}\" {}'.format(each_position,
                                      field_key_to_qry,
                                      table,
                                      where_cql)

        response = db_man.influx_qry(cql).get_points()

        time_on_db = pd.to_datetime([t for t in response][0]['time'])

        # adjust to lower minute
        # construct the answer as a dict
        ans[each_position.lower()] = one_minute_adjustment(time_on_db)

    return ans


def product_dict(dicts):
    """Cartesian product of a dictionary of lists
    https://stackoverflow.com/a/40623158/3512107

     >>> list(dict_product(dict(number=[1,2], character='ab')))
    [{'character': 'a', 'number': 1},
     {'character': 'a', 'number': 2},
     {'character': 'b', 'number': 1},
     {'character': 'b', 'number': 2}]
    """
    return (dict(zip(dicts, x)) for x in itertools.product(*dicts.values()))


def get_series_info(table):
    """Returns tags of each series in a table

    :return: list of dictionaries
    """
    # get series by symbols - provider tags - frequency (if bars)
    cql = 'SHOW TAG VALUES ON ' \
          '"securities_master" FROM "{}" ' \
          'WITH KEY IN (\"{}\", \"{}\", \"{}\")'.format(table,
                                                        'provider',
                                                        'symbol',
                                                        'frequency')

    response = db_man.influx_qry(cql).get_points()

    provs = []
    symb = []
    freq = []
    for resp in response:
        if resp['key'] == 'provider':
            provs.append(resp['value'])
        elif resp['key'] == 'symbol':
            symb.append(resp['value'])
        elif resp['key'] == 'frequency':
            freq.append(resp['value'])

    # the fx_ticks table does not include a frequency tag, I was aware of its
    # utility after all series were inserted. At the moment influx does not
    # support adding tags to existing series. A request is open:
    # https://github.com/influxdata/influxdb/issues/3904
    if not freq:
        freq.append('')

    series_tags = {'symbol': symb,
                   'provider': provs,
                   'frequency': freq}

    # construct all possibilities
    # Cartesian product of a dictionary of lists
    product_series_tags = product_dict(series_tags)

    ans = []
    for tag_product in product_series_tags:
        each_ans = dict()

        # add time bounds for each series
        bounds = time_bounds(table, tags=tag_product)

        each_ans['first_time'] = bounds['first']
        each_ans['last_time'] = bounds['last']
        each_ans['provider'] = tag_product['provider']
        each_ans['symbol'] = tag_product['symbol']
        each_ans['frequency'] = tag_product['frequency']

        # construct answer list of dictionaries
        ans.append(each_ans)

    return ans


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


def tick_resampling(input_table, output_tabl, tags, custom_dates=False,
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
            ticks = db_man.influx_qry(cql)[input_table]
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


def load_all_series(input_table, output_table, freq, overwrite):
    """ Load all series of a table and call resampling

    """

    # What series are in the tick table
    tick_series = get_series_info(input_table)
    # What series are in the bars table
    bar_series = get_series_info(output_table)

    for each_series in tick_series:

        tick_resampling(output_table_prefix='fx',
                        tags=tags_in_bar_series,
                        input_table=input_table,
                        custom_dates=custom_dates,
                        start_datetime=start_datetime)


    logger.info('Insert all series finished.')


def main():
    time0 = datetime.datetime.now()

    log_title("START LOADING MULTIPLE BAR SERIES")

    load_all_series(input_table='fx_ticks',
                    output_table='bars',
                    freq='1min',
                    overwrite=False)

    # tick_resampling(symbol='AUDCAD',
    #                 provider='fxcm',
    #                 input_table='fx_ticks',
    #                 custom_dates=False,
    #                 output_table_prefix='fx')

    time1 = datetime.datetime.now()
    logger.info('TOTAL RUNNING TIME WAS: {}'.format(time1 - time0))

if __name__ == '__main__':
    setup_logging()

    logger = logging.getLogger('tick_resampling')

    main()
