"""
Emit alerts for given instruments and levels

"""
from oanda.pricing.get import get_prices_once


def alert_factory(levels, interval=60):

    instruments_list = levels.keys()
    prices = get_prices_once(instruments_list)

    for instrument in instruments_list:
        print(prices.get(instrument))



if __name__ == "__main__":
    x = {'EUR_USD': [1.2015, 1.1998],
            'USD_JPY': [113.00]}

    alert_type = 'local'

    alert_factory(levels = x)
