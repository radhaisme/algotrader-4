# coding=utf-8
"""
https://docs.python.org/3/howto/logging.html
https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

"""

import logging
import logging.config
import pathlib

import yaml

from common.config import log_configuration, log_saving_path


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    path = pathlib.Path(log_configuration())

    if path.is_file():
        update_conf_file()
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def update_conf_file():
    """
    Update the logging configuration file with the saving path of the log files as defined in the CONFIG file

    """
    saving_path = pathlib.Path(log_saving_path())
    config_file = pathlib.Path(log_configuration())

    with open(config_file) as f:
        doc = yaml.load(f)

    doc['handlers']['info_file_handler']['filename'] = str(saving_path / 'bsk_info.log')
    doc['handlers']['error_file_handler']['filename'] = str(saving_path / 'bsk_error.log')

    with open(config_file, 'w') as f:
        yaml.dump(doc, f)


if __name__ == '__main__':
    setup_logging()


