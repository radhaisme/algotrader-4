# coding=utf-8
"""
https://docs.python.org/3/howto/logging.html
https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

"""
import logging
import logging.config
from logging import Formatter
import pathlib
import yaml
import time
from common.settings import ATSett


def setup_logging(default_level=logging.INFO):
    """Setup logging configuration

    :param default_level:
    :return:
    """
    path = ATSett().log_configuration()
    path = pathlib.Path(path)
    try:
        with open(path, 'rt') as my_file:
            config = yaml.safe_load(my_file.read())
        logging.config.dictConfig(config)
    except:
        logging.basicConfig(level=default_level)
        raise SystemError


def update_conf_file():
    """Update the logging configuration file with the paths
    defined in the CONFIG file
    """
    saving_path = pathlib.Path(setts.log_saving_path())
    config_file = pathlib.Path(setts.log_configuration())

    with open(config_file) as my_file:
        doc = yaml.load(my_file)

    doc['handlers']['info_file_handler']['filename'] = \
        str(saving_path / 'bsk_info.log')
    doc['handlers']['error_file_handler']['filename'] = \
        str(saving_path / 'bsk_error.log')

    with open(config_file, 'w') as my_file:
        yaml.dump(doc, my_file)


class UTCFormatter(Formatter):
    converter = time.gmtime


def log_test():
    x = 1
    print(x)
    logger.info('Var: %s', x)
    return x


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('log_config')
    log_test()
