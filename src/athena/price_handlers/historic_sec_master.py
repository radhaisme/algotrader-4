import datetime
import pandas as pd

from athena.price_handlers.base import AbstractTickPriceHandler
from common.utilities import iter_islast, previous_and_next
from databases.influx_manager import influx_client


class HistoricTickPriceHandler(AbstractTickPriceHandler):
    """
    HistoricTickPriceHandler is designed to read the Securities Master Database
    running on Influxdb and query for each requested symbol and time,
    providing an interface to obtain the "latest" tick in a manner identical to a live
    trading interface.

    """

    def __init__(self, symbols_list, data_provider, start_time, end_time):
        self.db_client = influx_client(client_type='dataframe')
        self.symbols_list = symbols_list
        self.data_provider = data_provider
        self.start_time = start_time
        self.end_time = end_time
        self._tick_data = pd.DataFrame()

        self._query_divider()

    def _divide_in_chuncks(self):
        total = pd.to_datetime(self.end_time, utc=True) - pd.to_datetime(self.start_time, utc=True)
        # When the range is longer than one hour
        if total.seconds >= 3600:
            range = pd.date_range(start=self.start_time,
                                  end=self.end_time,
                                  freq='1h',
                                  tz='UTC').tolist()
            #Add the last range
            diff = pd.to_datetime(self.end_time, utc=True) - range[-1]
            if diff.seconds > 0:
                range.append(pd.to_datetime(self.end_time, utc=True))
            elif diff.seconds < 0:
                del range[-1]
            return range
        # Smaller than one hour
        else:
            return [pd.to_datetime(self.start_time, utc=True),
                    pd.to_datetime(self.end_time, utc=True)]

    def _query_divider(self):

        queries_ranges = self._divide_in_chuncks()

        # Add a millisecond to the star time of each sub query
        for (prev, item, nxt), islast in iter_islast(previous_and_next(queries_ranges)):
            if prev is None:
                start_time_interm = item
            else:
                start_time_interm = item + datetime.timedelta(milliseconds=1)

            # Send a query request for each subquery
            if not islast:
                start_time_interm = start_time_interm.strftime('%Y-%m-%d %H:%M:%S.%f')
                nxt = nxt.strftime('%Y-%m-%d %H:%M:%S.%f')
                self._query_the_data(symbols,
                                     pd.to_datetime(start_time_interm),
                                     pd.to_datetime(nxt))

    def _query_the_data(self, symbols_list, s_time, e_time, table='fx_tick'):
        """
        Get tick data for a selected symbol over a period of time
        Args:
            symbols_list: the symbols to query about
            s_time: start datetime object/string
            e_time: end datetime object/string
            table: the measurement in the influx database

        Returns:

        """
        # CQL statement constructor for multiple symbols

        all_symbols = ""
        for each_symbol, islast in iter_islast(symbols_list):
            str_to_add = "symbol=\'{}\'".format(each_symbol)
            if islast:
                all_symbols += str_to_add
            else:
                all_symbols = all_symbols + str_to_add + ' OR '

        final_statement = "SELECT * " \
                          "FROM \"{}\" " \
                          "WHERE provider=\'{}\' " \
                          "AND time >= \'{}\' " \
                          "AND time < \'{}\' ".format(table,
                                                      self.data_provider,
                                                      s_time,
                                                      e_time)

        final_statement = final_statement + " AND (" + all_symbols + ")"

        ticks_to_add = self.db_client.query(query=final_statement)[table]

        self._tick_data = pd.concat([self._tick_data, ticks_to_add])

    def get_new_tick(self):
        """
        Returns the latest tick from the data feed.
        """

        for idx, row in ticks._tick_data.iterrows():
            yield (idx, row['bid'], row['ask'], row['symbol'], row['provider'])


if __name__ == '__main__':

    symbols = ['EURUSD']
    provider = 'fxcm'
    s_date = '2017-02-01 15:00:00.000'
    e_date = '2017-02-01 17:00:00.100'

    ticks = HistoricTickPriceHandler(symbols, provider, s_date, e_date)

    c = 0
    for t in ticks.get_new_tick():
        print('{}: {}'.format(c, t))
        c += 1
