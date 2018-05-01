# coding=utf-8

import datetime
import pandas as pd

from databases.influx_manager import influx_client


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


def tick_resampling(symbol, provider, input_table, output_table,
                    custom_dates=False, start_time=None, end_time=None):

    if custom_dates:
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)
    else:
        bounds = time_bounds(measurement=input_table, symbol=symbol, provider=provider)
        start_time = bounds['FIRST']
        end_time = bounds['LAST']

    client = influx_client(client_type='dataframe')
    delta = datetime.timedelta(hours=24)
    tags = {'provider': provider,
            'symbol': symbol}
    field_columns = ['open', 'high', 'low', 'close']
    protocol = 'json'
    while start_time < end_time:
        print('{} Doing: {}'.format(datetime.datetime.now(), start_time))
        partial_end = start_time + delta
        cql = 'SELECT time, bid, ask FROM {} ' \
              'WHERE symbol=\'{}\' ' \
              'AND provider=\'{}\' ' \
              'AND time>=\'{}\' ' \
              'AND time<\'{}\''.format(input_table, symbol, provider, start_time, partial_end)
        start_time = partial_end

        try:
            ticks = client.query(cql)[input_table]
            ticks['mid'] = ticks.mean(axis=1)
            ticks.drop(['bid', 'ask'], axis=1, inplace=True)

            bars = ticks.resample(rule='1Min', level=0).ohlc().ffill()
            # Drop multiindex, INflux write has problem with that
            bars.columns = bars.columns.droplevel(0)
            # Drop N/A. When there are no tick, do not create a bar
            bars.dropna(inplace=True)

            # Insert in database
            client.write_points(dataframe=bars,
                                measurement=output_table,
                                tags=tags,
                                time_precision='ms',
                                protocol=protocol,
                                numeric_precision='full',
                                field_columns=field_columns)
        except:
            print('Not data for {}'.format(start_time))

    client.close()

if __name__ == '__main__':
    symbol = 'EURUSD'
    provider = 'fxcm'
    table = 'fx_tick'
    output = 'fx_1m_EURUSD'


    sdate = '2018-02-01 00:00:00.000'
    edate = '2018-02-02 00:00:00.000'
    t0 = datetime.datetime.now()
    tick_resampling(symbol=symbol, provider=provider,
                    input_table=table, output_table=output,
                    custom_dates=False)
    t1 = datetime.datetime.now()

    print("Running for : {} ".format(t1-t0))