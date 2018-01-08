"""
Created on 30 Dec 2017

@author: Javier Garcia
"""

import quandl
from oanda_common import config


def get_treasury_curve_quandl(key, point, sdate='', edate=''):
    """
    """
    my_table = {'1m':1, '3m':2, '6m':3,
                '1y':4, '2y':5, '3y':6,
                '5y':7, '7y':8, '10y':9,
                '20y':10, '30y':11}

    ticket = "USTREASURY/YIELD." + str(my_table.get(point))
    
    print(ticket)
    return quandl.get(ticket, authtoken=key, start_date=sdate, end_date=edate)


if __name__ == '__main__':
    quandl_api_key = config('DEFAULT', 'quandl_api_key', 'str')
    
    my_data =get_treasury_curve_quandl(key=quandl_api_key, point='10y')
    print(my_data )




