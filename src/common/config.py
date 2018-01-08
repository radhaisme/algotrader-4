import os
import yaml

###############################################################
# ALL CONFIG SELECTIONS FROM HERE
###############################################################
CONFIG = os.environ['TRADE_CONF']


def oanda_connection_type():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['default']['oanda_conn']


def oanda_config():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['oanda_' + oanda_connection_type()]


def sql_config():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['sql']


