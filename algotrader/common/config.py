import os
import yaml
import pathlib


###############################################################
# ALL CONFIG SELECTIONS FROM HERE
###############################################################

# CONFIG = pathlib.Path(os.environ['TRADE_CONF'])
CONFIG = pathlib.Path("/home/javier/Dropbox/trading/trading.conf")


def oanda_connection_type():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['default']['oanda_conn']


def oanda_config():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['oanda_' + oanda_connection_type()]


def sql_config():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['sql']


def fxcm_data_path():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['fxcm_data']['hostname']


def store_originals_fxcm():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['fxcm_data']['store_originals']


def store_clean_fxcm():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['fxcm_data']['store_clean']


def influx_config():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['influx']


def log_configuration():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['logging']['config']


def log_saving_path():
    stream = open(CONFIG, 'rt')
    return yaml.load(stream)['logging']['saving_path']


