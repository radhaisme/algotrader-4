'''
Created on 29 Dec 2017

@author: Javier
'''

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.exc import SQLAlchemyError
from oanda_common import config


class DB:
    """
    Database object that manage interactions with database
    Variant of: https://stackoverflow.com/a/19440352/3512107
    """
    def __init__(self, db_name):
        self.conn = None
        self.meta = None
        self.engine = None
        self.db_name = db_name

    def db_engine(self):
        db = r"sqlite:///" + db_path(self.db_name)
        self.engine = create_engine(db, echo = False)

    def connect(self):
        try:
            self.conn = self.engine.connect()
        except (SQLAlchemyError, AttributeError):
            self.db_engine()
            self.conn = self.engine.connect()

    def query(self, sql):
        try:
            ans = self.conn.execute(sql)
        except (SQLAlchemyError, AttributeError):
            self.connect()
            ans = self.conn.execute(sql)
        return ans

    def table(self, name):
        return self.metadata().tables[name]

    def metadata(self):
        try:
            meta = MetaData(self.engine, reflect = True)
        except (SQLAlchemyError, AttributeError):
            self.db_engine()
            meta = MetaData(self.engine, reflect = True)
        return meta


if __name__ == '__main__':

    db = DB('forecast')

    sql = "SELECT score FROM scores"
    cur = db.query(sql)
    print(cur.fetchall())
    
    
    