#!/usr/bin/env python

from oanda.account.account import Account
from oanda.oanda_common.config import OandaContext


def account_details():
    """
    Create an API context, and use it to fetch and display the state of an
    Account.

    The configuration for the context and Account to fetch is parsed from the
    config file provided as an argument.
    """
    # Create empty context
    ctx = OandaContext()
    # Load configuration
    ctx.load_configuration()
    # Create API
    api = ctx.create_context()
    account_id = ctx.active_account
    # Fetch the details of the Account found in the config file
    response = api.account.get(account_id)

    # Extract the Account representation from the response.
    account = Account(response.get("account", "200"))
    account.dump()


if __name__ == "__main__":
    account_details()
