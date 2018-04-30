import os
import yaml

###############################################################
# ALL CONFIG SELECTIONS FROM HERE
###############################################################
# TODO: Environmental variable reading is not working in the remote ubuntu server, with this.
# TODO: configuration file is defined at environmental variable 'TRADE_CONF' that must
# TODO: be already set up.
# TODO: CONFIG = os.environ['TRADE_CONF']

# TODO point to the configuration file directly, must review to work with a ENV VAR
CONFIG = r'Z:\Javier\Dropbox\trading\trading.conf'


def oanda_connection_type():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['default']['oanda_conn']


def oanda_config():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['oanda_' + oanda_connection_type()]


def sql_config():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['sql']


def fxcm_data_path():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['fxcm_data']['hostname']


def influx_config():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['influx']


def cassandra_config():
    stream = open(os.path.expanduser(CONFIG), 'r')
    return yaml.load(stream)['cassandra']
