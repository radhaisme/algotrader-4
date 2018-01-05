import common.view
from common.config import OandaContext


def main():
    """
    Create an API context, and use it to fetch and display the tradeable 
    instruments for and account.

    The configuration for the context and Account to fetch is parsed from the
    configuration file provided as an argument.
    """
    # Create empty context
    ctx = OandaContext('demo')
    # Load configuration
    ctx.load_configuration()
    # Create API
    api = ctx.create_context()

    # Fetch the tradeable instruments for the Account
    account_id = ctx.active_account
    response = api.account.instruments(account_id)

    # Extract the list of Instruments from the response.
    instruments = response.get("instruments", "200")

    instruments.sort(key = lambda i: i.name)

    def marginFmt(instrument):
        return "{:.0f}:1 ({})".format(1.0 / float(instrument.marginRate),
                                      instrument.marginRate)

    def pipFmt(instrument):
        location = float(10 ** instrument.pipLocation)
        return "{:.4f}".format(location)

    # Print the details of the Account's tradeable instruments
    common.view.print_collection(
            "{} Instruments".format(len(instruments)),
            instruments,
            [
                ("Name", lambda i: i.name),
                ("Type", lambda i: i.type),
                ("Pip", pipFmt),
                ("Margin Rate", marginFmt),
                ])


if __name__ == "__main__":
    main()
