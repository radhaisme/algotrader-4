# -*- coding: utf-8 -*-
# https://medium.com/netflix-techblog/scaling-time-series-data-storage-part-i-ec2b6d44ba39
# https://www.influxdata.com/blog/influxdb-vs-cassandra-time-series/
# Something to read when working with a lot of data
# https://www.dataquest.io/blog/pandas-big-data/
import pandas as pd
import datetime
import logging
import pathlib
import sys

from gzip import open
from influxdb.exceptions import *
from pytz import utc

from databases.influx_manager import influx_client
from data_acquisition.fxmc import in_store
from common.config import store_clean_fxcm
from log.logging import setup_logging


def prepare_data_for_securities_master(file_path):
    """Opens a file downloaded from FXMC and format it to upload to Securities Master databases


    Args:
        file_path: path

    Returns: pandas DF
    """

    file_path = pathlib.Path(file_path)
    filename = file_path.parts[-1][:-7]
    logger.info('Preparing CSV file: {}'.format(filename))
    try:
        df = pd.read_csv(filepath_or_buffer=file_path,
                         compression='gzip',
                         sep=',',
                         skiprows=1,
                         names=['price_datetime', 'bid', 'ask'],
                         parse_dates=[0],
                         date_parser=my_date_parser,
                         index_col=[0],
                         float_precision='high',
                         engine='c')
    except OSError:
        logger.exception('Error reading file {}'.format(filename))
        sys.exit(-1)

    logger.info('File: {} ready for insert'.format(filename))
    return df


def my_date_parser(date_string):
    """Manual date parse
    from '%Y-%m-%d %H:%M:%s.%f'  to datetime UTC

    :param date_string:
    :return:
    """
    return datetime.datetime(int(date_string[6:10]),        # %Y
                             int(date_string[:2]),          # %m
                             int(date_string[3:5]),         # %d
                             int(date_string[11:13]),       # %H
                             int(date_string[14:16]),       # %M
                             int(date_string[17:19]),       # %s
                             int(date_string[20:]) * 1000,  # %f
                             tzinfo=utc)


def writer(client, data, tags, into_table):
    """Insert tick data into securities master database.

    :param client:
    :param data:
    :param tags:
    :param into_table:
    :return:
    """
    protocol = 'json'
    field_columns = ['bid', 'ask']
    logger.info('Insert data for: {}'.format(tags['filename']))
    try:
        client.write_points(dataframe=data,
                            measurement=into_table,
                            protocol=protocol,
                            field_columns=field_columns,
                            tags=tags,
                            time_precision='u',
                            numeric_precision='full',
                            batch_size=10000)
        logger.info('Data insert OK for {}'.format(tags['filename']))
    except (InfluxDBServerError, InfluxDBClientError):
        logger.exception('Error data insert - {}'.format(tags['filename']))
        sys.exit(-1)


def insert_validation(filepath, table, tags, validation_type, abs_tolerance=10):
    """Validate number of rows: CSV vs Database


    """
    client = influx_client(client_type='dataframe')

    if validation_type == 'full':
        cql = 'SELECT COUNT(bid) FROM {} ' \
              'WHERE filename=\'{}\' ' \
              'AND provider=\'{}\' ' \
              'AND symbol=\'{}\''.format(table,
                                         tags['filename'],
                                         tags['provider'],
                                         tags['symbol'])
    elif validation_type == 'fast':
        cql = 'SELECT row_count, difference FROM {} ' \
              'WHERE filename=\'{}\' ' \
              'AND provider=\'{}\' ' \
              'AND symbol=\'{}\''.format(table + '_validation',
                                         tags['filename'],
                                         tags['provider'],
                                         tags['symbol'])
    else:
        logger.exception(
            'Validation type must be either \'full\' or \'fast\'. Correct the parameter and lunch the function again.')
        raise SystemExit

    try:
        cql_ans = client.query(query=cql)
        if validation_type == 'full':
            rows_in_db = cql_ans[table]['row_count'].iloc[0]
            row_count = sum(1 for _r in open(pathlib.Path(filepath), 'r')) - 1

            # Now that a full validation was performed update the validation table
            update_validation_table(client, tags, rows_in_db, row_count - rows_in_db)

        elif validation_type == 'fast':
            rows_in_db = cql_ans[table + '_validation']['row_count'].iloc[0]
            row_count = rows_in_db + cql_ans[table + '_validation']['difference'].iloc[0]

    except KeyError:
        if validation_type == 'full':
            logger.info('Data from {} not in database'.format(tags['filename']))
            client.close()
            row_count = sum(1 for _r in open(pathlib.Path(filepath), 'r')) - 1
            return {'value': 'Not in DB', 'csv': row_count, 'sec_master': 0, 'diff': row_count}
        elif validation_type == 'fast':
            # if there is no info in the validation table performs full validation, even if fast was selected.
            client.close()
            return insert_validation(filepath=filepath, table=table, tags=tags,
                                     validation_type='full', abs_tolerance=10)

    difference = row_count - rows_in_db
    if difference == 0:
        ans = 'Exact'
    elif abs(difference) > abs_tolerance:
        ans = 'Not Acceptable'
    else:
        ans = 'Acceptable'

    logger.info('Validation {} difference of {} - validation type: {}'.format(ans, difference, validation_type))
    return {'value': ans, 'csv': row_count, 'sec_master': rows_in_db, 'diff': difference}


def update_validation_table(client, tags,  row_count, difference):
    """Updates a validation table with information for a light row_count validation

    """
    protocol = 'json'
    field_columns = ['row_count', 'difference']

    data = {'row_count': [row_count],
            'difference': [difference]}
    df = pd.DataFrame(data=data, index=[datetime.datetime.utcnow()])

    # creates a normal database client and deletes any previous series with matching tags
    c = influx_client()
    c.delete_series(tags=tags)
    c.close()

    # update the validation information
    try:
        client.write_points(dataframe=df,
                            measurement='fx_ticks_validation',
                            protocol=protocol,
                            field_columns=field_columns,
                            tags=tags)

    except (InfluxDBServerError, InfluxDBClientError):
        logger.exception('Error data insert in validation table')
        sys.exit(-1)


def delete_series(tags):
    """Deletes series in current database

    :param tags:
    :return:
    """
    logger.info('Deleting previous series: {}'.format(tags['filename']))
    try:
        client = influx_client()
        client.delete_series(tags=tags)
        client.close()
        logger.info('Series {} deleted.'.format(tags['filename']))
    except (InfluxDBClientError, InfluxDBServerError):
        logger.exception('Could not delete series {}'.format(tags['filename']))


def prepare_and_insert(db_client, file_path, tags, into_table, validation):

    if validation != 'Not in DB':
        # delete series with same tags in current database
        delete_series(tags)

    # Turn the CSV into a dataframe ready for insert
    data = prepare_data_for_securities_master(file_path=file_path)
    # insert the data to sec master database
    writer(client=db_client, data=data, tags=tags, into_table=into_table)


def load_multiple_tick_files(dir_path, provider, into_table, validation_type, overwrite=True):
    """ Iterates over a directory and load all the .gz files with tick data from FXCM.
        Files names must match the regex: "^[A-Z]{6}_20\d{1,2}_\d{1,2}.csv.gz"

    :param dir_path: str
    :param provider: str
    :param into_table: str
    :param validation_type: 'full' or 'fast'
    :param overwrite: Boolean
    :return:
    """

    # Define the set of files to work with
    # uses the in_store() function that verifies the name of the file to
    # math the regex: "^[A-Z]{6}_20\d{1,2}_\d{1,2}.csv.gz"
    dir_path = pathlib.Path(dir_path)
    files = in_store(dir_path)[0]

    # Connect to securities master database
    db_client = influx_client(client_type='dataframe')

    # Define what is considered an error (absolute difference) and define separated log for these errors.
    error_tolerance = 10

    # Loop each file in directory
    for each_file in files:
        # Get some basic information about the data
        symbol = each_file.parts[-1][:6]
        filename = each_file.parts[-1][:-7]

        tags = {'symbol': symbol,
                'provider': provider,
                'filename': filename}

        logger.info('Working on {}'.format(filename))

        # Validate if data already is in securities master database.
        # Number of data points in CSV must be similar (+/- tolerance) to database
        # to be considered as already inserted.
        pre_validation = insert_validation(filepath=each_file,
                                           table=into_table,
                                           tags=tags,
                                           validation_type=validation_type,
                                           abs_tolerance=error_tolerance)

        if overwrite or pre_validation['value'] == 'Not Acceptable' or pre_validation['value'] == 'Not in DB':
            prepare_and_insert(db_client=db_client, file_path=each_file,
                               tags=tags, into_table=into_table, validation=pre_validation['value'])

            # Performance post insert validation that data is ok in database
            # Influx has some trouble with the milliseconds and sometimes drops some data.
            # Some tolerance is acceptable. Check the validation function for info.
            post_validation = insert_validation(filepath=each_file,
                                                table=into_table,
                                                tags=tags,
                                                validation_type=validation_type,
                                                abs_tolerance=error_tolerance)

            if post_validation['value'] == 'Exact' or post_validation['value'] == 'Acceptable':
                logger.info('Successful insert for {}: {} '
                            'data points with {} difference'.format(filename, post_validation['sec_master'],
                                                                    post_validation['diff']))
            else:
                logger.error('Error insert for {}: {} difference'.format(filename, post_validation['diff']))

        else:
            logger.info('Data for {} already in database:'
                        ' {} data points with {} difference'
                        ' - Validation type: {}'.format(filename,
                                                        pre_validation['sec_master'],
                                                        pre_validation['diff'],
                                                        validation_type))

    logger.info('All data files processed!')


def multiple_file_insert():

    my_dir = pathlib.Path(store_clean_fxcm())
    t0 = datetime.datetime.now()

    logger.info('#'*90)
    logger.info('########################### START LOADING MULTIPLE TICK FILES ############################')
    logger.info('#' * 90)

    load_multiple_tick_files(dir_path=my_dir, provider='fxcm', into_table='fx_ticks',
                             validation_type='fast', overwrite=False)
    t1 = datetime.datetime.now()
    logger.info('TOTAL RUNNING TIME WAS: {}'.format(t1 - t0))


def file_insert_validation():
    csv_file_str = r"/media/javier/My Passport/Trading/data/clean_fxcm/TO_LOAD/AUDCAD_2015_17.csv.gz"
    csv_file_path = pathlib.Path(csv_file_str)
    the_filename = csv_file_path.parts[-1][:-7]
    the_table = 'fx_ticks'
    the_provider = 'fxcm'

    cql = 'SELECT * FROM {} ' \
          'WHERE filename=\'{}\' ' \
          'AND provider=\'{}\''.format(the_table,
                                       the_filename,
                                       the_provider)

    db_client = influx_client(client_type='dataframe')

    data_db = db_client.query(cql)[the_table]
    del data_db['filename']
    del data_db['provider']
    del data_db['symbol']
    data_db.index.name = 'price_datetime'
    data_db.sort_index(axis=1, inplace=True)

    data_csv = prepare_data_for_securities_master(csv_file_path)['data']
    data_csv.sort_index(axis=1, inplace=True)

    index_csv = data_csv.index
    index_db = data_db.index
    return index_csv.difference(index_db)


def insert_one_series():
    tags = {'filename': 'AUDCAD_2015_15',
            'provider': 'fxcm',
            'symbol': 'AUDCAD'}

    client = influx_client(client_type='dataframe')
    file_path = pathlib.Path(
        '/media/javier/My Passport/Trading/data/clean_fxcm/LOADED/AUDCAD/2015/AUDCAD_2015_15.csv.gz')
    row_count = sum(1 for _r in open(file_path, 'r')) - 1
    prepare_and_insert(db_client=client, file_path=file_path, tags=tags, into_table='fx_ticks', validation=True,
                       row_count=row_count)


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('FXCM LOADING INTO DATABASE')
    multiple_file_insert()

    # insert_one_series()

