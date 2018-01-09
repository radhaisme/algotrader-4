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
                               instruments = ','.join(instruments),
                               since = None,
                               includeUnitsAvailable = False
                               )

    # create answer dictionary
    ans = {}
    for instr in instruments:
        ans[instr] = None

    # Populate answer dictionary with prices newer than the latest time
    # seen in a price
    inner_ans = {}
    for price in response.get("prices", 200):
        inner_ans['time'] = price.time
        inner_ans['bid'] = price.bids[0].price
        inner_ans['ask'] = price.asks[0].price
        ans[price.instrument] = inner_ans

    return ans


def yield_prices(instruments_list, poll_interval):
    # Poll for of prices
    while True:
        yield get_prices_once(instruments_list)
        time.sleep(poll_interval)


if __name__ == "__main__":

    instrument_list = ["EUR_USD", "EUR_JPY", "GBP_USD"]
    my_yield_prices = yield_prices(instrument_list, 60)

    for x in my_yield_prices:
        print(x)
