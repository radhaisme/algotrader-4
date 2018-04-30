# coding=utf-8

from cassandra.cluster import Cluster
from cassandra.cqlengine.management import create_keyspace_simple
from cassandra import DriverException
from common.config import cassandra_config


def cassandra_client():
    """
    Instantiate a connection to the Cassandra Database.

    Returns: Cassandra client

    """
    config = cassandra_config()
    host = config['host']
    port = config['port']
    print(host[0],":",port)
    #keyspace = config['database']
    return Cluster(contact_points=host, port=port).connect()


def create_keyspace():

    qry = "CREATE KEYSPACE IF NOT EXISTS kong WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };"
    client = cassandra_client()
    client.execute(qry)





if __name__ == '__main__':
    create_keyspace()
