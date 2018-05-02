# coding=utf-8
"""
https://docs.python.org/3/howto/logging.html
https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

"""

import logging.config
import pathlib
import yaml
import logging
from common.config import log_configuration


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    path = pathlib.Path(log_configuration())

    if path.is_file():
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


# def test_fun():
#     for n in range(10):
#         logger.info('print: {}'.format(n))
#     print('End')
#
#
# def other_test():
#     try:
#         open('/path/to/does/not/exist', 'rb')
#     except (SystemExit, KeyboardInterrupt):
#         raise
#     except Exception as e:
#         logger.exception('M¡XXXXXXXXXXXXXXXXXXXXXX')
#
#
# def main():
#     logger = logging.getLogger(__name__)
#     test_fun()
#     # other_test()
#
# if __name__ == '__main__':
#     setup_logging()
#     main()

