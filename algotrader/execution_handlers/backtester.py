# coding=utf-8

import logging
import queue
from price_handlers.historic_sec_master import HistoricFxTickPriceHandler, HistoricBarPriceHandler
from log.logging import setup_logging
import datetime


class Backtester:
    """
    Encapsulates the settings and components for carrying out
    an event-driven backtest.
    """
    def __init__(self, strategy, provider, symbol_list, start_time,
                 end_time, frequency, initial_capital, execution_handler, portfolio):

        self.strategy = strategy
        self.provider = provider
        self.symbol_list = symbol_list
        self.start_time = start_time
        self.end_time = end_time
        self.data_handler = None
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
            pass
            # self.data_handler = HistoricBarPriceHandler(data_provider=self.provider, symbols_list=self.symbol_list,
            #                                             start_time=self.start_time, end_time=self.end_time,
            #                                             events_queue=self.events_queue, frequency=self.frequency)

    def run_the_queue(self):

        while True:
            self.data_handler.stream_next()
            event = self.events_queue.get()
            print(event)





if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('Backtester')

    start = datetime.datetime.now()
    symbols = ['AUDCHF', 'AUDCAD']
    data_p = 'fxcm'
    s_date = '2018-02-06 15:00:00'
    e_date = '2018-02-06 15:05:00'

    test = Backtester(strategy=None, provider=data_p, symbol_list=symbols, start_time=s_date, end_time=e_date,
                      frequency='ticks', initial_capital=100, execution_handler=None, portfolio=None)

    test.run_the_queue()


