
import queue

from athena.price_handlers.base import AbstractTickPriceHandler
from databases.influx_manager import influx_client


class HistoricTickPriceHandler(AbstractTickPriceHandler):
    """
    HistoricTickPriceHandler is designed to read the Securities Master Database
    running on Influxdb and query for each requested symbol and time,
    providing an interface to obtain the "latest" bar in a manner identical to a live
    trading interface.

    """

    def __init__(self, events_queue, db_client, symbol_list, data_provider, start_time, end_time):
        self.db_client = db_client
        self.events_queue = events_queue
        self.symbol_list = symbol_list
        self.data_provider = data_provider
        self.start_time = start_time
        self.end_time = end_time
        self.symbol_data = {}

        self._get_the_data()

    def _query_one_symbol(self, symbol, s_time, e_time):
        table = 'fx_tick'
        q_statement = "SELECT *::field FROM \"{}\" WHERE provider=\'{}\' AND " \
                      "symbol=\'{}\' AND time >= \'{}\' AND time < \'{}\'".format(table,
                                                                                  self.data_provider,
                                                                                  symbol,
                                                                                  s_time,
                                                                                  e_time)
        print(q_statement)
        clients = self.db_client
        rs = clients.query(q_statement)
        return rs.get_points()

    def _get_the_data(self):
        for each_symbol in symbols:
            self.symbol_data[each_symbol] = self._query_one_symbol(symbol=each_symbol,
                                                                   s_time=self.start_time,
                                                                   e_time=self.end_time)



if __name__ == '__main__':

    my_queue = queue.Queue()
    client = influx_client()
    symbols = ['EURUSD', 'GBPUSD']
    provider = 'fxcm'
    s_date = '2014-12-28 00:22:00.000'
    e_date = '2014-12-28 00:22:05.000'

    x = HistoricTickPriceHandler(my_queue, client, symbols, provider, s_date, e_date)

    print(x)
