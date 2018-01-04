'''
Created on 4 Jan 2018

@author: Javier
'''

from common.config import OandaContext
from account.account import Account


def main():
    
    ctx = OandaContext('demo')
    ctx.load_configuration()
    api = ctx.create_context()
    
    account_id = ctx.active_account
    
    response = api.account.summary(account_id)
    
    summary = response.get("account", "200")
    account = Account(summary)

    account.dump()





if __name__ == '__main__':
    main()