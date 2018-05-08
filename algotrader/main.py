import datetime
from databases.loader_from_fxcm import load_multiple_tick_files
from log.logging import setup_logging
import logging


def main():

    t0 = datetime.datetime.now()
    my_dir = r"D:\Trading\data\clean_fxcm\LOADED"
    load_multiple_tick_files(dir_path=my_dir, provider='fxcm', into_table='fx_ticks')
    t1 = datetime.datetime.now()

    logger.info('TOTAL RUNNING TIME WAS: {}'.format(t1-t0))


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)

    main()

