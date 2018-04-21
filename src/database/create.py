"""
Created on 29 Dec 2017

:author: Javier Garcia
"""
import sqlalchemy
from sqlalchemy import Column, Integer, Numeric, String, DateTime, ForeignKey
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from common.config import sql_config

Base = declarative_base()

class SqlEngine:
    """
    Engine object with all the requirement to connect to MySQL
    """

    def __init__(self):
        """
        Initialize an empty configuration object
        """
        self.dialect = None
        self.connector = None
        self.server = None
        self.port = None
        self.user = None
        self.password = None
        self.dbname = None
        self.echo = True
        self.path = None

    def load_configuration(self, **kwargs):
        """

        """
        self.dialect = kwargs.get('dialect', self.dialect)
        self.connector = kwargs.get('connector', self.connector)
        self.server = kwargs.get('server', self.server)
        self.password = kwargs.get('password', self.password)
        self.port = kwargs.get('port', self.port)
        self.user = kwargs.get('user', self.user)
        self.dbname = kwargs.get('dbname', self.dbname)
        self.echo = kwargs.get('echo', self.echo)

        self.validate()

    def create_engine(self):
        """
        Initialize engine to MySQL database for a given configuration
        """
        instruction = '{0}+{1}://{2}:{3}@{4}:{5}/{6}?charset=utf8mb4'.format(self.dialect,
                                                                             self.connector,
                                                                             self.user,
                                                                             self.password,
                                                                             self.server,
                                                                             self.port,
                                                                             self.dbname)
        try:
            engine = sqlalchemy.create_engine(instruction, echo=self.echo, encoding="utf8")
            print('Engine created successfully.  --  ' + str(engine))
            return engine
        except:
            print(
                'There is a problem with the engine creation - Check configuration '
                'or '
                'server')

    def validate(self):
        """
        Ensure configuration is valid
        """
        errors = []

        if self.dialect is None:
            errors.append('dialect')
        if self.connector is None:
            errors.append('connector')
        if self.user is None:
            errors.append('user')
        if self.password is None:
            errors.append('password')
        if self.server is None:
            errors.append('server')
        if self.port is None:
            errors.append('port')
        if self.dbname is None:
            errors.append('dbname')

        if len(errors) > 0:
            print('Configuration file has error in:')
            for e in errors:
                print('    - ' + e)
        else:
            print('Configuration file is OK.')


class Exchange(Base):
    """
    Exchanges object
    """
    __tablename__ = 'exchanges'
    id_exchange = Column(Integer, primary_key=True)
    name = Column(String(255))
    city = Column(String(255))
    country = Column(String(255))
    currency = Column(String(255))
    timezone = Column(String(255))
    created_date = Column(DateTime)
    last_updated_date = Column(DateTime)
    children = relationship("Symbol")


class DataVendor(Base):
    """
    Data vendor object
    """
    __tablename__ = 'vendors'
    id_vendor = Column(Integer, primary_key=True)
    name = Column(String(255))
    website = Column(String(255))
    email = Column(String(255))
    created_date = Column(DateTime)
    last_updated_date = Column(DateTime)
    children = relationship("stock_daily_prices", 'fxmc_tick_data')


class Symbol(Base):
    """
    Symbol object
    """
    __tablename__ = 'symbols'
    symbol = Column(String(32), primary_key=True)
    id_exchange = Column(Integer, ForeignKey('exchanges.id_exchange'))
    asset_class = Column(String(64))
    base_currency = Column(String(64))
    quote_currency = Column(String(64))
    created_date = Column(DateTime)
    last_updated_date = Column(DateTime)
    children = relationship("stock_daily_prices", 'fxmc_tick_data')


class StockDailyPrice(Base):
    """
    Daily Prices
    """
    __tablename__ = 'stock_daily_prices'
    id_daily_price = Column(Integer, primary_key=True)
    id_vendor = Column(Integer, ForeignKey('vendors.id_vendor'))
    last_update = Column(DateTime)
    symbol = Column(String(32), ForeignKey('symbols.symbol'))
    price_date = Column(DateTime)
    close_price = Column(Numeric(19, 6))
    adj_close_price = Column(Numeric(19, 6))
    high_price = Column(Numeric(19, 6))
    low_price = Column(Numeric(19, 6))
    open_price = Column(Numeric(19, 6))
    volume = Column(Integer)


class FxmcData(Base):
    """
    Tick Data from FXMC
    """
    __tablename__ = 'fxcm_data'
    symbol = Column(String(32), ForeignKey('symbols.symbol'), primary_key=True)
    price_date = Column(DateTime, primary_key=True)
    id_vendor = Column(Integer, ForeignKey('vendors.id_vendor'))
    bid = Column(Numeric(19, 6))
    ask = Column(Numeric(19, 6))
    last_update = Column(DateTime)
    # For reference: the url of the source file used in the last update
    # https://stackoverflow.com/a/219664/3512107
    source_file = Column(mysql.VARCHAR(2083))


class OandaData(Base):
    """
    Tick Data from Oanda
    """
    __tablename__ = 'oanda_data'
    symbol = Column(String(32), ForeignKey('symbols.symbol'), primary_key=True)
    price_date = Column(DateTime, primary_key=True)
    id_vendor = Column(Integer, ForeignKey('vendors.id_vendor'))
    bid = Column(Numeric(19, 6))
    ask = Column(Numeric(19, 6))
    last_update = Column(DateTime)


# class EcoSymbol(Base):
#     """
#     Economic symbol object
#     """
#     __tablename__ = 'eco_symbols'
#     id_eco_symbol = Column(Integer, primary_key=True)
#     ticket = Column(String(32))
#     country = Column(String(32))
#     human_name = Column(String(64))
#     currency = Column(String(64))
#     children = relationship("EcoData")
#
#
# class EcoData(Base):
#     """
#     Economic data object
#     """
#     __tablename__ = 'economic_data'
#     id_eco_data = Column(Integer, primary_key=True)
#     id_vendor = Column(Integer, ForeignKey('vendors.id_vendor'))
#     id_eco_symbol = Column(Integer, ForeignKey('eco_symbols.id_eco_symbol'))
#     price_date = Column(DateTime)
#     created_date = Column(DateTime)
#     close_price = Column(Numeric(19,6))

def create_db(**kwargs):
    """

    """
    instruction = '{0}+{1}://{2}:{3}@{4}:{5}'.format(kwargs['dialect'],
                                                     kwargs['connector'],
                                                     kwargs['user'],
                                                     kwargs['password'],
                                                     kwargs['server'],
                                                     kwargs['port'])
    db_engine = sqlalchemy.create_engine(instruction, echo=kwargs['echo'])

    # Query for existing databases
    existing_databases = db_engine.execute("SHOW DATABASES;")
    existing_databases = [d[0] for d in existing_databases]

    # Create database if not exists
    if kwargs['dbname'] not in existing_databases:
        db_engine.execute('CREATE DATABASE ' + kwargs['dbname'])
        print("Created database {0}".format(kwargs['dbname']))
    else:
        print('Database already exist.')


def create_conn(**kwargs):
    """
    """
    instruction = '{0}+{1}://{2}:{3}@{4}:{5}/{6}'.format(kwargs['dialect'],
                                                         kwargs['connector'],
                                                         kwargs['user'],
                                                         kwargs['password'],
                                                         kwargs['server'],
                                                         kwargs['port'],
                                                         kwargs['dbname'])

    return sqlalchemy.create_engine(instruction, echo=kwargs['echo'])


#
#
# def create_symbol(current_session, **kwargs):
#     pass
#


def update_schema():
    """

    Returns: A new database with the configuration given in the .conf file

    """
    check = input('This will create a new database. Shall we proceed? "yes/no: ')

    if check == 'yes':
        # Get SQL configuration from .conf file
        config = sql_config()

        # Create a new empty database
        create_db(**config)

        # Create engine that connect to the new database
        conn = SqlEngine()
        conn.load_configuration(**config)
        my_engine = conn.create_engine()

        # Apply the metadata in the declarative base
        # Table creation
        Base.metadata.create_all(my_engine)

        # Tell me what tables are there
        print('\nTables at database:')
        meta = Base.metadata
        for table in meta.tables.keys():
            print('     ' + table)
    else:
        print('Mission aborted')


if __name__ == '__main__':
    # Create a new database with the declarative schema written here
    update_schema()

    # Session = sessionmaker(bind=conn)
    # session = Session()
    #
    # print(session)
    #
    # vendor = {'name':'QUANDL', 'website':'www.quandl.com', 'email':'',
    #               'created_date': '2017-12-31'}
    #
    # symbol = {'name':'fx', 'city':'', 'country':'',
    #           'currency':'', 'timezone' :'', 'created_date':'2017-12-31'}
    #
    # exchange = {}
    #
    #
    # #create_vendor(session, **vendor)
