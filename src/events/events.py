"""

:Author:
"""

from events import Event


class MarketEvent(Event):
    """
    Handles the events of receiving a new market update with
    corresponding bars.
    """

    def __init__(self, datetime, symbol, bid, ask, provider, tradeable):
        """
        Initializes the MarketEvent.
        """
        self.type = 'MARKET'
        self.datetime = datetime
        self.symbol = symbol
        self.bid = bid
        self.ask = ask
        self.provider = provider
        self.tradeable = tradeable

    def __str__(self):
        """
        Returns: Human readable description of object
        """
        return "Type: {}, Symbol:{}, Datetime:{}, Bid{}, Ask{}".format(self.type,
                                                                       self.symbol,
                                                                       self.datetime,
                                                                       self.bid,
                                                                       self.ask)

    def __repr__(self):
        return str(self)
