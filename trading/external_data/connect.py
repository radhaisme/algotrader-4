# -*- coding: utf-8 -*-
'''

Created on 29 Dec 2017

@author: Javier Garcia
'''
from common import config
import quandl


def time_series(database, dataset, column, sdate='', edate=''):
    #full_ticket = ticket + '.' + str(column)    
    ans = quandl.get(database_code=database, dataset_code=dataset, column_index = column, 
                     start_date=sdate, end_date=edate)
    return ans



if __name__ == '__main__':
    my_quandl_api = config(section='DEFAULT', key='quandl_api_key')
    quandl.ApiConfig.api_key = my_quandl_api
    
    database = 'USTREASURY'
    dataset = 'YIELD'
    col = 10
    my_data = time_series(database= database, dataset=dataset, column=col)

    print(my_data)
    
