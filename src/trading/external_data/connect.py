# -*- coding: utf-8 -*-
'''

Created on 29 Dec 2017

@author: Javier Garcia
'''
from common import config
import quandl
import pandas as pd
import matplotlib.pyplot as plt

def time_series(dbase, dset, col, sdate='', edate=''):
    """
    
    """
    full_ticket = dbase + '/' + dset + '.' + str(col)    
    return quandl.get(full_ticket, start_date=sdate, end_date=edate)
    




if __name__ == '__main__':
    my_quandl_api = config(section='DEFAULT', key='quandl_api_key')
    quandl.ApiConfig.api_key = my_quandl_api
    
    database = 'USTREASURY'
    dataset = 'YIELD'
    column = 10
    ten_yr = time_series(dbase= database, dset=dataset, col=9)
    two_yr = time_series(dbase= database, dset=dataset, col=5)
    

    ans = pd.concat([two_yr, ten_yr], axis=1)
    spread = pd.Series(ans['10 YR'] -ans['2 YR'], name='spread')
                       
    ans = pd.concat([ans, spread], axis=1)

    spread.plot()
    plt.show()
    
    
    
    
    
    
    