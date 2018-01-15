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


def data_storage_path():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['storage']['data_store']

def fxcm_data_path():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['fxcm_data']['hostname']
