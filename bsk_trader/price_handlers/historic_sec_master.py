import datetime

from common.utilities import iter_islast
from databases.influx_manager import influx_client


class HistoricTickPriceHandler:
    """
    HistoricTickPriceHandler is designed to read the Securities Master Database
    running on Influxdb and query for each requested symbol and time,
    providing an interface to obtain the "latest" tick in a manner identical to a live
    trading interface.

    """

    def __init__(self, symbols_list, data_provider, start_time, end_time):
        self.db_client = influx_client()
        self.symbols_list = symbols_list
        self.data_provider = data_provider
        self.start_time = start_time
        self.end_time = end_time

        self._tick_data = None
        self._tick_table = 'fx_tick'

        self. _database_query(self.symbols_list, self.start_time, self.end_time)

    @staticmethod
    def is_tick(self):
        return True

    @staticmethod
    def is_bar(self):
        return False

    def _query_constructor(self, symbols_list, s_time, e_time):
        """
        CQL statement constructor for multiple symbols
        """
        all_symbols = ''
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
                          "AND time < \'{}\' ".format(self._tick_table,
                                                      self.data_provider,
                                                      s_time,
                                                      e_time)

        return final_statement + " AND (" + all_symbols + ")"

    def _database_query(self, symbols_list, s_time, e_time):
        """
        Get tick data for a selected symbol over a period of time
        Args:
            symbols_list: the symbols to query about
            s_time: start datetime object/string
            e_time: end datetime object/string

        Returns: Influx ResultSet

        """
        final_statement = self._query_constructor(symbols_list, s_time, e_time)
        self._tick_data = self.db_client.query(query=final_statement,
                                               chunked=True,
                                               chunk_size=1000)

    def get_new_tick(self):
        """
        Generator that returns the latest tick from the data feed.
        """
        return self._tick_data.get_points()


class HistoricBarPriceHandler:
    """
    HistoricBarPriceHandler is designed to read the Securities Master Database
    running on Influxdb and query for each requested symbol and time,
    providing an interface to obtain the "latest" bar in a manner identical to a live
    trading interface.

    """

    def __init__(self, symbols_list, timeframe, data_provider, start_time, end_time):
        self.db_client = influx_client()
        self.symbols_list = symbols_list
        self.timeframe = timeframe
        self.data_provider = data_provider
        self.start_time = start_time
        self.end_time = end_time

    def query_tick_data(self):
        tick_data = HistoricTickPriceHandler(self.symbols_list,
                                             self.data_provider,
                                             self.start_time,
                                             self.end_time)


if __name__ == '__main__':
    start = datetime.datetime.now()
    symbols = ['EURUSD']
    provider = 'fxcm'
    s_date = '2017-02-01 15:00:00.000'
    e_date = '2017-02-01 15:01:00.100'

    ticks = HistoricTickPriceHandler(symbols, provider, s_date, e_date)


    c = 0
    for t in ticks.get_new_tick():
        print('{}: {}'.format(c, t))
        c += 1

