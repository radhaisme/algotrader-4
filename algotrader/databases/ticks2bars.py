# coding=utf-8
"""
Converts ticks to bars
"""
import datetime
import itertools
import logging

import pandas as pd

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
    logger.info('Querying series info in table \'{}\''.format(table))

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

        each_ans['first'] = bounds['first']
        each_ans['last'] = bounds['last']
        each_ans['provider'] = tag_product['provider']
        each_ans['symbol'] = tag_product['symbol']
        each_ans['frequency'] = tag_product['frequency']

        # construct answer list of dictionaries
        ans.append(each_ans)

    return ans


def ticks_to_bars(ticks, freq):
    """Re sample ticks [timestamp bid, ask) to bars OHLC of selected frequency
    https://stackoverflow.com/a/17001474/3512107
    :param ticks:
    :param freq:
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

    bars = ticks.resample(rule=freq, level=0).ohlc()

    # Drop N/A. When there are no tick, do not create a bar
    bars.dropna(inplace=True)

    # Drop multi-index, Influx write has problem with that
    bars.columns = bars.columns.droplevel(0)

    return bars


def tick_resampling(input_table, output_table, tags, start_datetime,
                    end_datetime):
    """Re sample tick data in securities master to desired frequency

    """
    # Define the time extension of each query.
    # The bigger the number, more RAM needed.
    delta = datetime.timedelta(hours=24)

    # Construct the intervals to obtain the data in chunks
    chuncks = []
    init_dt = start_datetime
    while init_dt < end_datetime:

        end_dt = init_dt + delta
        chuncks.append((init_dt, end_dt))

        init_dt = end_dt

    # works with each query: re sampling and insert
    for each_chunck in chuncks:
        logger.info('Re sampling {} from {} delta {}'.format(tags.values(),
                                                             each_chunck[0],
                                                             delta))
        cql = 'SELECT time, bid, ask FROM {} ' \
              'WHERE symbol=\'{}\' ' \
              'AND provider=\'{}\' ' \
              'AND time>=\'{}\' ' \
              'AND time<\'{}\''.format(input_table,
                                       tags['symbol'],
                                       tags['provider'],
                                       each_chunck[0],
                                       each_chunck[1])

        # Get the ticks requested
        response = db_man.influx_qry(client_type='dataframe', cql=cql)

        # check for the weekends --no data--
        if not response:
            logger.warning('No data for {} at {}'.format(tags.values(),
                                                         each_chunck[0]))
        else:
            ticks = response[input_table]

            # Call the re sampling function
            bars = ticks_to_bars(ticks=ticks, freq=tags['frequency'])

            # Insert into securities master database
            field_keys = ['open', 'high', 'low', 'close']

            db_man.influx_writer(data=bars,
                                 field_columns=field_keys,
                                 tags=tags,
                                 into_table=output_table)


def date_comparison(input_series, output_series):
    """ Compare the start and end dates for the input and output series and
    obtain the dates that completes the output series

    :param input_series:
    :param output_series:
    :return:
    """
    first_input = input_series['first']
    last_input = input_series['last']
    first_output = output_series['first']
    last_output = output_series['last']

    # if output series is not left aligned, something happened and the series
    #  must be re insert
    if first_input != first_output:
        ans = {'first': first_input,
               'last': last_input}
    else:
        ans = {'first': last_output,
               'last': last_input}
    return ans

def load_all_series(input_table, output_table, freq):
    """ Load all series of a table and call resampling

    """

    # What series are in the tick table
    tick_series = get_series_info(input_table)
    # What series are in the bars table
    bar_series = get_series_info(output_table)

    for each_tick_series in tick_series:
        tick_symbol = each_tick_series['symbol']
        tick_provider = each_tick_series['provider']

        start_datetime = each_tick_series['first']
        end_datetime = each_tick_series['last']

        # Get the dates for resampling function
        for each_bar_series in bar_series:
            bar__symbol = each_bar_series['symbol']
            bar__provider = each_bar_series['provider']
            # find id tick series are in bars
            if tick_symbol == bar__symbol and tick_provider == bar__provider:
                # check the date in the two series ticks vs bars
                cross_dates = date_comparison(input_series=each_tick_series,
                                              output_series=each_bar_series)
                start_datetime = cross_dates['first']
                end_datetime = cross_dates['last']
            else:
                start_datetime = each_tick_series['first']
                end_datetime = each_tick_series['last']

        # Get the tags for the new series
        tags4bar = {'provider': tick_provider,
                    'symbol': tick_symbol,
                    'frequency': freq}

        # Do the re sampling
        tick_resampling(output_table=output_table,
                        input_table=input_table,
                        tags=tags4bar,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime)

    logger.info('Insert all series finished.')


def main():
    """ Call for running all.
    """
    log_title("START LOADING MULTIPLE BAR SERIES")
    load_all_series(input_table='fx_ticks',
                    output_table='bars',
                    freq='1min')
    logger.info('ticks to bars end running.')


if __name__ == '__main__':
    setup_logging()

    logger = logging.getLogger('tick2bars')

    main()
