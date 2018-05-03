# coding=utf-8

import queue
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
            self.data_handler =
