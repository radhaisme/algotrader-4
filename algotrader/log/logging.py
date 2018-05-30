# coding=utf-8
"""
https://docs.python.org/3/howto/logging.html
https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

"""
import logging
import logging.config
import pathlib
import yaml
from common.settings import ATSett

setts = ATSett()


def setup_logging(default_level=logging.INFO):
    """Setup logging configuration

    :param default_level:
    :return:
    """

    path = pathlib.Path(setts.log_configuration())
    if path.is_file():
        # Saving path update
        update_conf_file()
        # read configuration
        with open(path, 'rt') as my_file:
            config = yaml.safe_load(my_file.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


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


if __name__ == '__main__':
    setup_logging()
