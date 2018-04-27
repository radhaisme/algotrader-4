
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

    def __init__(self, events_queue, db_client, symbols_list, data_provider, start_time, end_time):
        self.db_client = db_client
        self.events_queue = events_queue
        self.symbols_list = symbols_list
        self.data_provider = data_provider
        self.start_time = start_time
        self.end_time = end_time
        self.symbol_data = {}

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

        symbol_str = "AND (symbol=\'{}\' OR symbol=\'{}\')".format(symbols_list[0], symbols_list[1])

        q_statement1 = "SELECT * " \
                       "FROM \"{}\" " \
                       "WHERE provider=\'{}\' " \
                       "AND time >= \'{}\' " \
                       "AND time < \'{}\' ".format(table,
                                                   self.data_provider,
                                                   s_time,
                                                   e_time)



        final_statement = q_statement1 + " " + symbol_str
        print(final_statement)
        clients = self.db_client
        rs = clients.query(final_statement)
        print(rs)
        return rs.get_points()


    def get_symbol_ticks(self, symbol):
        """
        Tick data for one of the symbols in the input list
        Args:
            symbol:

        Returns: tick price generator

        """
        return self.symbol_data[symbol]

    def get_all_sync__ticks(self):
        pass


if __name__ == '__main__':

    my_queue = queue.Queue()
    client = influx_client()
    symbols = ['EURUSD', 'AUDCAD', 'USDJPY']
    provider = 'fxcm'
    s_date = '2018-02-01 15:01:00.000'
    e_date = '2018-02-01 15:01:01.000'

    x = HistoricTickPriceHandler(my_queue, client, symbols, provider, s_date, e_date)

