
import queue
import datetime
from athena.price_handlers.base import AbstractTickPriceHandler
from databases.influx_manager import influx_client
from common.utilities import iter_islast


class HistoricTickPriceHandler(AbstractTickPriceHandler):
    """
    HistoricTickPriceHandler is designed to read the Securities Master Database
    running on Influxdb and query for each requested symbol and time,
    providing an interface to obtain the "latest" tick in a manner identical to a live
    trading interface.

    """

    def __init__(self, db_client, symbols_list, data_provider, start_time, end_time):
        self.db_client = db_client
        self.symbols_list = symbols_list
        self.data_provider = data_provider
        self.start_time = start_time
        self.end_time = end_time
        self._tick_data = ''

        self._query_the_data(self.symbols_list, self.start_time, self.end_time)

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

        clients = self.db_client
        self._tick_data = clients.query(final_statement).get_points()

    def get_new_tick(self):
        """
        Returns the latest tick from the data feed.
        """
        return self._tick_data



if __name__ == '__main__':

    my_queue = queue.Queue()
    client = influx_client()
    symbols = ['EURUSD', 'AUDCAD', 'AUDJPY', 'CADCHF', 'EURGBP']
    provider = 'fxcm'
    s_date = '2018-02-01 15:00:00.000'
    e_date = '2018-02-01 15:00:10.000'

    ticks = HistoricTickPriceHandler(client, symbols, provider, s_date, e_date)


    c = 0
    for tick in ticks._tick_data:
        print("{}: {}".format(c, tick))
        c += 1
