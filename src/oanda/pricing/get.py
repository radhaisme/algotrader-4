#!/usr/bin/env python

import time
from oanda.oanda_common.config import OandaContext


def get_prices_once(instruments):
    """
    Get the prices for a list of Instruments for the active Account.
    Repeatedly poll for newer prices if requested.
    """
    # Create empty context
    ctx = OandaContext()
    # Load configuration
    ctx.load_configuration()
    # Create API
    api = ctx.create_context()

    account_id = ctx.active_account

    # Query Oanda
    response = api.pricing.get(account_id,
                               instruments=','.join(instruments),
                               since=None,
                               includeUnitsAvailable=False)

    # Populate answer dictionary with prices newer than the latest time
    # seen in a price
    ans = {}
    for price in response.get("prices", 200):
        inner_ans = {'time': price.time,
                     'bid': price.bids[0].price,
                     'ask': price.asks[0].price}
        ans[price.instrument] = inner_ans

    return ans


def yield_prices(instruments_list, poll_interval):
    # Poll for of prices
    while True:
        yield get_prices_once(instruments_list)
        time.sleep(poll_interval)


if __name__ == "__main__":
    instrument_list = ["EUR_USD", "EUR_JPY", "GBP_USD"]
    my_prices = get_prices_once(instrument_list)
    print(my_prices)
