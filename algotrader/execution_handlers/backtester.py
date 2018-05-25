# coding=utf-8

import queue
from price_handlers.historic_sec_master import HistoricFxTickPriceHandler
from log.logging import setup_logging
import logging
import datetime


class Backtester():
    """
    Encapsulates the settings and components for carrying out
    an event-driven backtest.
    """
    def __init__(self, source_directory, strategy, symbol_list, start_time,
                 end_time, price_handler, timeframe, initial_capital, execution_handler, portfolio):

        self.source_directory = source_directory
        self.strategy = strategy
        self.symbol_list = symbol_list
        self.start_time = start_time
        self.end_time =end_time
        self.data_handler = price_handler
        self.timeframe = timeframe
        self.initial_capital = initial_capital
        self.execution_handler = execution_handler
        self.portfolio = portfolio

        self.events_queue = queue.Queue()

    def _generate_components_instances(self):
        try:
            print('Instantiating data handler')
            self.data_handler =1
        except:
            pass


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('Backtester')

    start = datetime.datetime.now()
    symbols = ['AUDCHF', 'AUDCAD']
    provider = 'fxcm'
    s_date = '2018-02-06 15:00:00'
    e_date = '2018-02-06 15:05:00'

    ticks = HistoricFxTickPriceHandler(symbols, provider, s_date, e_date)


    c = 0
    for t in ticks.get_new_tick():
        print('{}: {}'.format(c, t))
        c += 1

