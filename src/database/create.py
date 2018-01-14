"""
Created on 29 Dec 2017

:author: Javier Garcia
"""
import sqlalchemy
from sqlalchemy.sql import exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Numeric, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
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
        instruction = '{0}+{1}://{2}:{3}@{4}:{5}/{6}'.format(self.dialect,
                                                             self.connector,
                                                             self.user,
                                                             self.password,
                                                             self.server,
                                                             self.port,
                                                             self.dbname)
        try:
            engine = sqlalchemy.create_engine(instruction, echo = self.echo)
            print('Engine created successfully.  --  '+str(engine))
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
                print('    - '+e)
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
    children = relationship("Symbol")


class Symbol(Base):
    """
    Symbol object
    """
    __tablename__ = 'symbols'
    symbol = Column(String(32), primary_key=True)
    id_exchange = Column(Integer, ForeignKey('vendors.id_vendor'))
    asset_class = Column(String(64))
    base_currency = Column(String(64))
    quote_currency = Column(String(64))
    created_date = Column(DateTime)
    last_updated_date = Column(DateTime)
    children = relationship("DailyPrice", 'TickFXPrice')


class DailyPrice(Base):
    """
    Daily Prices
    """
    __tablename__ = 'daily_prices'
    id_daily_price = Column(Integer, primary_key=True)
    id_vendor = Column(Integer, ForeignKey('vendors.id_vendor'))
    symbol = Column(String(32), ForeignKey('symbols.symbol'))
    price_date = Column(DateTime)
    created_date = Column(DateTime)
    last_updated_date = Column(DateTime)
    close_price = Column(Numeric(19, 6))
    adj_close_price = Column(Numeric(19, 6))
    high_price = Column(Numeric(19, 6))
    low_price = Column(Numeric(19, 6))
    open_price = Column(Numeric(19, 6))
    volume = Column(Integer)


class TickFXPrice(Base):
    """
    Daily Prices
    """
    __tablename__ = 'tick_fx_prices'
    id_tick_fx_price = Column(Integer, primary_key=True)
    id_vendor = Column(Integer, ForeignKey('vendors.id_vendor'))
    symbol = Column(String(32), ForeignKey('symbols.symbol'))
    price_date = Column(DateTime)
    created_date = Column(DateTime)
    last_updated_date = Column(DateTime)
    bid = Column(Numeric(19, 6))
    ask = Column(Numeric(19, 6))



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
    '''
    '''
    instruction = '{0}+{1}://{2}:{3}@{4}:{5}/{6}'.format(kwargs['dialect'],
                                                         kwargs['conector'],
                                                         kwargs['user'],
                                                         kwargs['password'],
                                                         kwargs['server'],
                                                         kwargs['port'],
                                                         kwargs['dbname'])
    
    return sqlalchemy.create_engine(instruction, echo=kwargs['echo'])
    

def create_vendor(current_session, **kwargs):
    """
    Create/Update a vendor 
    """
    # Checking if registered
    stmt = exists().where(DataVendor.name == kwargs['name'])
    qry =  current_session.query(stmt)
    is_registered = qry.scalar()

    # DataVendor object
    # Parse created_date to Datetime
    created_date = datetime.strptime(kwargs['created_date'], '%Y-%m-%d')
    vendor_to_add = DataVendor(name = kwargs['name'], website = kwargs['website'], email = kwargs['email'], 
                               created_date = created_date, last_updated_date = datetime.now())
    
    if is_registered:
        # Registered vendor
        print('Data Vendor {} already exists.'.format(kwargs['name']))
        proceed_with_update = input('Proceed with update? (yes/no): ' )
        
        if proceed_with_update == 'yes':
            # Delete previous entries and add new
            current_session.query(DataVendor).filter_by(name=kwargs['name'] ).delete()
            current_session.add(vendor_to_add)
            current_session.commit()
            print('Data Vendor {} updated.'.format(kwargs['name']))
        elif proceed_with_update == 'no':
            # Do nothing
            print('Update for {} skipped.'.format(kwargs['name']))
    else:
        # Create new vendor
        current_session.add(vendor_to_add)
        current_session.commit()
        print('New Data Vendor {} added to database.'.format(kwargs['name']))
            
    
def create_symbol(current_session, **kwargs):
    pass


def create_exchange(current_session, **kwargs):
    pass


def create_new_schema():
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
    # ONLY use when you want to reinitialize the database
    # BE CAUTIOUS THIS DELETE DATA
    create_new_schema()

    

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
