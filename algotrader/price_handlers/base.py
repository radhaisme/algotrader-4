
from abc import ABCMeta


class AbstractPriceHandler(object):
    """
    PriceHandler is a base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) PriceHandler object is to output a set of
    TickEvents or BarEvents for each financial instrument and place
    them into an event queue.

    This will replicate how a live strategy would function as current
    tick/bar data would be streamed via a brokerage. Thus a historic and live
    system will be treated identically by the rest of the QSTrader suite.
    """

    __metaclass__ = ABCMeta

    def unsubscribe_symbol(self, symbol):
        """
        Unsubscribe the price handler from a current symbol symbol.
        """
        try:
            self.symbol.pop(symbol, None)
            self.symbol_data.pop(symbol, None)
        except KeyError:
            print("Could not unsubscribe symbol {} as it was never subscribed.".format(str(symbol)))

    def get_last_timestamp(self, symbol):
        """
        Returns the most recent actual timestamp for a given symbol
        """
        if symbol in self.symbol:
            timestamp = self.symbol[symbol]["timestamp"]
            return timestamp
        else:
            print("Timestamp for symbol {} is not available from {}.".format(symbol,
                                                                             self.__class__.__name__))
            return None

class AbstractTickPriceHandler(AbstractPriceHandler):

    def is_tick(self):
        return True

    def is_bar(self):
        return False

    def _store_event(self, event):
        """
        Store price event for bid/ask
        """
        symbol = event.symbol
        self.symbol[symbol]["bid"] = event.bid
        self.symbol[symbol]["ask"] = event.ask
        self.symbol[symbol]["timestamp"] = event.time

    def get_best_bid_ask(self, ticker):
        """
        Returns the most recent bid/ask price for a ticker.
        """
        if symbol in self.symbol:
            bid = self.symbol[symbol]["bid"]
            ask = self.symbol[symbol]["ask"]
            return bid, ask
        else:
            print(
                "Bid/ask values for ticker %s are not "
                "available from the PriceHandler." % symbol
            )
            return None, None


class AbstractBarPriceHandler(AbstractPriceHandler):
    def is_tick(self):
        return False

    def is_bar(self):
        return True

    def _store_event(self, event):
        """
        Store price event for closing price and adjusted closing price
        """
        symbol = event.symbol
        self.symbol[symbol]["close"] = event.close_price
        self.symbol[symbol]["adj_close"] = event.adj_close_price
        self.symbol[symbol]["timestamp"] = event.time

    def get_last_close(self, symbol):
        """
        Returns the most recent actual (unadjusted) closing price.
        """
        if symbol in self.symbol:
            close_price = self.symbol[symbol]["close"]
            return close_price
        else:
            print(
                "Close price for ticker %s is not "
                "available from the YahooDailyBarPriceHandler."
            )
            return None
