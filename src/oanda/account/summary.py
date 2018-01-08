"""
Created on 4 Jan 2018

:author: Javier Garcia
"""

from oanda_common.config import OandaContext
from oanda.account.account import Account

from common.config import oanda_connection_type


def main():
    """
    Create an API context, and use it to fetch and display an Account summary.

    The configuration for the context and Account to fetch is parsed from the
    configuration file provided as an argument.
    """
    # Create empty context
    ctx = OandaContext(oanda_connection_type())
    
    # Load configuration
    ctx.load_configuration()
    
    # Create api
    api = ctx.create_context()
    
    # Query for account summary
    account_id = ctx.active_account
    response = api.account.summary(account_id)
    
    # Get and parse the response
    summary = response.get("account", "200")
    my_account = Account(summary)

    # Print out nicely
    my_account.dump()


if __name__ == '__main__':
    main()
