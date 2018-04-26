#https://stackoverflow.com/questions/43876906/schema-for-tick-data-on-cassandra
import pandas as pd
from cassandra.cluster import Cluster


def cassandra_session():

    cluster = Cluster()
    session = cluster.connect()
    return session


def prepare_data_for_securities_master(file_path, temp_path):
    """
    Opens a file downloaded from FXMC and format it to upload to Securities Master database
    Returns: temp CSV

    """

    df = pd.read_csv(filepath_or_buffer=file_path,
                     compression='gzip',
                     sep=',',
                     skiprows=1,
                     names=['temp', 'bid', 'ask'],
                     parse_dates=[0],
                     date_parser=pd.to_datetime,
                     float_precision='high')

    df['provider'] = 'fxcm'
    df['symbol'] = 'AUDCAD'
    df['trade_date'] = df["temp"].dt.date
    df['trade_time'] = df["temp"].dt.time

    df.to_csv(path_or_buf=temp_path,
              encoding='utf-8',
              index=False,
              columns=['provider', 'symbol', 'trade_date', 'trade_time', 'bid', 'ask'])


def write_to_database(file_path):
    cql = "COPY prueba.fx_tick_data (provider,symbol, trade_date, trade_time, bid, ask) FROM " \
          + file_path + "WITH HEADER = TRUE;"
    session = cassandra_session()
    session.execute(cql)

    # COPY is only shell cqlsh not a CQL protocol.
    # importing would have to be coded by myself
    # https://stackoverflow.com/questions/33622930/copy-can-only-be-used-in-cqlsh-and-so-i-cant-use-it-as-a-cql-command-in-java


if __name__ == '__main__':
    #cassandra_client()

    my_file = "/media/sf_D_DRIVE/Trading/data/example_data/small.csv.gz"
    temp_file = "/media/sf_D_DRIVE/Trading/data/example_data/temp.csv"

    prepare_data_for_securities_master(my_file, temp_file)

    write_to_database(temp_file)