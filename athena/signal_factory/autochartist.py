"""

Uses REST API until Oanda publish V20 version

"""
import pandas as pd
import datetime as dt
from tabulate import tabulate
from oanda.oanda_common.oandapy import API  # REST API HERE
from common.config import oanda_config, oanda_connection_type
from time import time


def get_favorites():
    """
    Get Autochartist favorite technical signals, through Oanda Rest API

    """

    config = oanda_config()
    token = config.get('token')

    # Create API using oandapy wrapper for REST API
    oanda = API(environment = str(oanda_connection_type()), access_token = token)

    # Parameters
    payload = {}
    # Get signals
    ans = oanda.get_autochartist(param = payload)
    return ans


def arrange_signals(response):
    """
    Convert response from Oanda Autochartist to simpler DF structure for analysis

    Args:
        response: dict from json from response

    Returns: Simpler DF

    """
    signals = response.get('signals')
    ans = []
    idx = []

    for each_signal in signals:
        meta = each_signal.get('meta')
        signal_data = each_signal.get('data')
        s_id = each_signal.get('id')
        if each_signal.get('type') == 'chartpattern':
            signal_dict = {'instrument': each_signal.get('instrument'),
                           'score_quality': meta['scores']['quality'],
                           'score_breakout': meta['scores']['breakout'],
                           'score_initialtrend': meta['scores']['initialtrend'],
                           'score_clarity': meta['scores']['clarity'],
                           'score_uniformity': meta['scores']['uniformity'],
                           'score_mean': float(sum(meta['scores'].values())) / len(
                                   meta['scores']),
                           'pattern': meta['pattern'],
                           'completed': meta['completed'],
                           'interval': meta['interval'],
                           'direction': meta['direction'],
                           'probability': meta['probability'],
                           'length': meta['length'],
                           'stats_symbol': meta['historicalstats']['symbol']['percent'],
                           'stats_pattern': meta['historicalstats']['pattern']['percent'],
                           'stats_hour': meta['historicalstats']['hourofday']['percent'],
                           'trend_type': meta['trendtype'],
                           'end_time': dt.datetime.fromtimestamp(
                                   signal_data['patternendtime']),
                           'time_since_end': dt.timedelta(seconds = (
                                   time() - signal_data['patternendtime'])),
                           'resistance_y0': signal_data['points']['resistance']['y0'],
                           'resistance_x0': dt.datetime.fromtimestamp(
                                   signal_data['points']['resistance']['x0']),
                           'resistance_y1': signal_data['points']['resistance']['y1'],
                           'resistance_x1': dt.datetime.fromtimestamp(
                                   signal_data['points']['resistance']['x0']),
                           'support_y0': signal_data['points']['support']['y0'],
                           'support_x0': dt.datetime.fromtimestamp(
                                   signal_data['points']['support']['x0']),
                           'support_y1': signal_data['points']['support']['y1'],
                           'support_x1': dt.datetime.fromtimestamp(
                                   signal_data['points']['support']['x1']),
                           'prediction_low': signal_data['prediction']['pricelow'],
                           'prediction_high': signal_data['prediction']['pricehigh'],
                           'time_from': dt.datetime.fromtimestamp(
                                   signal_data['prediction']['timefrom']),
                           'time_to': dt.datetime.fromtimestamp(
                                   signal_data['prediction']['timeto'])
                           }
            ans.append(signal_dict)
            idx.append(s_id)

        elif each_signal.get('type') == 'keylevel':
            print('there is keylevel')

    return pd.DataFrame(ans, index = idx)


def print_out():
    favorites = get_favorites()
    signal_df = arrange_signals(favorites).sort_values(by = 'time_since_end',
                                                       ascending = False)

    # filtered_signals = signal_df.loc[signal_df['interval'] <= 60]
    print(tabulate(signal_df, headers = 'keys', tablefmt = 'simple'))


if __name__ == "__main__":

    print_out()
