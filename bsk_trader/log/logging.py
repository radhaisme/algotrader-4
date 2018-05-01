# coding=utf-8
"""
https://docs.python.org/3/howto/logging.html
https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

"""

import logging.config
import pathlib
import yaml


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """

    cwd = pathlib.Path.cwd()
    path = cwd / 'log_config.yaml'

    if path.is_file():
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        print(config)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


if __name__ == '__main__':
    # setup_logging()
