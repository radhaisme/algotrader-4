# -*- coding: utf-8 -*-
'''

Created on 29 Dec 2017

@author: Javier Garcia
'''
from common import config
import quandl


def time_series(ticket, column='', sdate='', edate=''):
    full_ticket = ticket + '.' + str(column)    
    ans = quandl.get(full_ticket, start_date=sdate, end_date=edate)
    return ans



if __name__ == '__main__':
    my_quandl_api = config(section='DEFAULT', key='quandl_api_key')
    quandl.ApiConfig.api_key = my_quandl_api
    
    my_ticket = 'USTREASURY/YIELD'
    col = 10
    my_data = time_series(my_ticket, col)

    print(my_data)
    
