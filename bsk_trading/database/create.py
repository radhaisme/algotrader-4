'''
Created on 29 Dec 2017

@author: Javier
'''

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from common import config


Base = declarative_base()
    
    
class Exchange(Base):
    __tablename__ = 'exchanges'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    city = Column(String(255))
    country = Column(String(255))
    currency = Column(String(255))
    timezone = Column(String(255))
    created_date = Column(DateTime)
    last_updated_date= Column(DateTime)
    
class DataVendor(Base):
    __tablename__ = 'vendors'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    website = Column(String(255))
    email = Column(String(255))
    created_date = Column(DateTime)
    last_updated_date= Column(DateTime)
    

class Symbol(Base):
    __tablename__ = 'symbols'


class DailyPrice(Base):
    __tablename__ = 'daily_prices'
    

def create_db(**kwargs):
    """
    
    """
    instruction = '{0}+{1}://{2}:{3}@{4}:{5}'.format(kwargs['dialect'], 
                                                         kwargs['conector'], 
                                                         kwargs['user'], 
                                                         kwargs['psw'], 
                                                         kwargs['server'],
                                                         kwargs['port'])
    db_engine= sqlalchemy.create_engine(instruction, echo=kwargs['echo'])
    
    # Query for existing databases
    existing_databases = db_engine.execute("SHOW DATABASES;")
    existing_databases = [d[0] for d in existing_databases]
    
    # Create database if not exists
    if kwargs['dbname'] not in existing_databases:
        db_engine.execute('CREATE DATABASE '+ kwargs['dbname'])
        print("Created database {0}".format(kwargs['dbname']))
    else:
        print('Database already exist.') 

  
def create_conn(**kwargs):
    '''
    '''
    instruction = '{0}+{1}://{2}:{3}@{4}:{5}/{6}'.format(kwargs['dialect'], 
                                                         kwargs['conector'], 
                                                         kwargs['user'], 
                                                         kwargs['psw'], 
                                                         kwargs['server'],
                                                         kwargs['port'], 
                                                         kwargs['dbname'])
    return sqlalchemy.create_engine(instruction, echo=kwargs['echo'])
    

if __name__ == '__main__':

    mysql_conf = {'dialect':config(section='MYSQL', key='dialect', key_type='str'),
                  'conector':config(section='MYSQL', key='conector', key_type='str'),
                  'server':config(section='MYSQL', key='server', key_type='str'),
                  'port':config(section='MYSQL', key='port', key_type='str'),
                  'user':config(section='MYSQL', key='user', key_type='str'),
                  'psw':config(section='MYSQL', key='password', key_type='str'),
                  'dbname':config(section='MYSQL', key='dbname', key_type='str'),
                  'echo':config(section='MYSQL', key='echo', key_type='bool')}
    
    
    create_db(**mysql_conf)
    conn = create_conn(**mysql_conf)
    Base.metadata.create_all(conn)
    
    
    
    
