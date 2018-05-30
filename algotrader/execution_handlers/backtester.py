# coding=utf-8

import queue
from price_handlers.historic_sec_master import HistoricFxTickPriceHandler, HistoricBarPriceHandler
from log.logging import setup_logging
import logging
import datetime


class Backtester:
    """
    Encapsulates the settings and components for carrying out
    an event-driven backtest.
    """
    def __init__(self, source_directory, strategy, provider, symbol_list, start_time,
                 end_time, price_handler, frequency, initial_capital, execution_handler, portfolio):

        self.source_directory = source_directory
        self.strategy = strategy
        self.provider = provider
        self.symbol_list = symbol_list
        self.start_time = start_time
        self.end_time = end_time
        self.data_handler = price_handler
        self.frequency = frequency
        self.initial_capital = initial_capital
        self.execution_handler = execution_handler
        self.portfolio = portfolio

        self.events_queue = queue.Queue()

        logger.info('Instantiating {} data handler'.format(self.frequency))
        if self.frequency == 'ticks':
            self.data_handler = HistoricFxTickPriceHandler(data_provider=self.provider, symbols_list=self.symbol_list,
                                                           start_time=self.start_time, end_time=self.end_time,
                                                           events_queue=self.events_queue)
        else:
            self.data_handler = HistoricBarPriceHandler(data_provider=self.provider, symbols_list=self.symbol_list,
                                                        start_time=self.start_time, end_time=self.end_time,
                                                        events_queue=self.events_queue, frequency=self.frequency)


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('Backtester')

    start = datetime.datetime.now()
    symbols = ['AUDCHF', 'AUDCAD']
    data_p = 'fxcm'
    s_date = '2018-02-06 15:00:00'
    e_date = '2018-02-06 15:05:00'
    my_q = queue.Queue()

    ticks = HistoricFxTickPriceHandler(symbols, data_p, s_date, e_date, my_q)

    for tick in ticks._get_streaming_ticks():
        print(tick)


