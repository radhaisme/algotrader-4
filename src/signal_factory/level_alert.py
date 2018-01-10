"""
Emit alerts for given instruments and levels

"""
from oanda.pricing.get import get_prices_once
import time


def alert_factory(levels, interval=10):

    instruments_list = levels.keys()

    # Flag alerts if active
    flags = {x: True for x in instruments_list}

    def check(instrument_list):
        # Get current prices
        prices = get_prices_once(instrument_list)
        # Check for conditions
        for instrument in instruments_list:
            if flags[instrument]:
                current_price = prices.get(instrument)
                target = levels[instrument]['level']

                if levels[instrument]['position'] == 'over' and current_price[
                    'ask'] > target:
                    flags[instrument] = False
                    print('alert {} ask over {}'.format(instrument, str(target)))
                elif levels[instrument]['position'] == 'under' and current_price[
                    'bid'] < target:
                    flags[instrument] = False
                    print('alert {} bid under {}'.format(instrument, str(target)))

    # Verify if any flag is True
    x = 0
    for flag in flags:
        if flags[flag]:
            x += 1

    # Set global flag
    if x >= 1:
        global_flag = True
    else:
        global_flag = False

    # Run while global flag
    while global_flag:
        time.sleep(interval)
        check(instruments_list)


if __name__ == "__main__":
    x = {'EUR_USD': {
        'level': 1.199,
        'position': 'over'
        },
        'USD_JPY': {
            'level': 114,
            'position': 'under'
            }
        }

    # alert_factory(levels = x)


