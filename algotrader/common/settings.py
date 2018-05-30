import logging
import os
import pathlib

import yaml

############################################################################
#               ALL CONFIG SELECTIONS FROM HERE                            #
############################################################################

# General setting to run the program, as database or broker connection info.
SYSTEM_SETTINGS = 'TRADE_CONF'
# Settings regarding a trading strategy
STRATEGY_SETTINGS = None


class ATSett:
    """General configuration options of AlgoTrader System."""
    def __init__(self, get_from='env'):
        """
        :param get_from: 'env' for environmental variable
                         'file' string pointing to conf file
        """
        self.get_from = get_from
        self.stream = self._stream()
        self.config_path = None

    # Internal functions
    def _path_from_env(self):
        try:
            env_value = os.environ[SYSTEM_SETTINGS]
            self.config_path = pathlib.Path(env_value)
        except (KeyError, FileNotFoundError, AttributeError):
            # TODO revise exceptions here
            logging.exception('Environmental Variable not correctly '
                              'configured or no present.')
            raise SystemError

    def _path_from_file(self):
        raise NotImplementedError

    def _stream(self):
        if self.get_from == 'env':
            self._path_from_env()
        elif self.get_from == 'file':
            self._path_from_file()

        stream = open(self.config_path, 'rt')
        return yaml.load(stream)

    # Broker access functions
    def oanda_connection_type(self):
        return self.stream['default']['oanda_conn']

    def oanda_config(self):
        return self.stream['oanda_' + self.oanda_connection_type()]

    # Databases management functions
    def sql_config(self):
        return self.stream['sql']

    def influx_config(self):
        return self.stream['influx']

    # Data storing management
    def fxcm_data_path(self):
        return self.stream['fxcm_data']['hostname']

    def store_originals_fxcm(self):
        return self.stream['fxcm_data']['store_originals']

    def store_clean_fxcm(self):
        return self.stream['fxcm_data']['store_clean']

    # Logging configuration
    def log_configuration(self):
        return self.stream['logging']['config']

    def log_saving_path(self):
        return self.stream['logging']['saving_path']


class StratSett:
    """Strategy configuration options

    """
    def __init__(self):
        raise NotImplementedError


