# -*- coding: utf-8 -*-
"""
Created on 3 Jan 2018

@author: Javier
"""
import yaml
import os
import sqlalchemy
import v20

# Get configuration path from enviromental variable
CONFIG = os.environ['TRADE_CONF']


class OandaContext:
    """
    Context API object of V20 Oanda engine
    """

    def __init__(self, connection_type):
        """
        Initialize an empty context object
        """
        self.connection_type = connection_type
        self.hostname = None
        self.streaming_hostname = None
        self.port = 443
        self.ssl = True
        self.token = None
        self.username = None
        self.accounts = []
        self.active_account = None
        self.path = None
        self.datetime_format = "RFC3339"

    def load_configuration(self, path = CONFIG):
        """
        Load Yaml file with configuration from path

        Args:
            path: path to yaml configuration file
        """
        self.path = path
        print('Loading from {}'.format(self.path))
        with open(os.path.expanduser(self.path), 'r') as stream:
            try:
                # Here must specify demo or live oanda section of the Yaml file
                y = yaml.load(stream)['oanda_'+self.connection_type]

                self.hostname = y.get('hostname', self.hostname)
                self.streaming_hostname = y.get('streaming_hostname',
                                                self.streaming_hostname)
                self.port = y.get('port', self.port)
                self.ssl = y.get('ssl', self.ssl)
                self.token = y.get('token', self.token)
                self.username = y.get('username', self.username)
                self.accounts = y.get('accounts', self.accounts)
                self.active_account = y.get('active_account', self.active_account)

                self.validate()
            except yaml.YAMLError as exc:
                print(exc)

    def create_context(self):
        """
        Initialize an API context based on the configuration instance
        """
        try:
            ctx = v20.Context(self.hostname,
                              self.port,
                              self.ssl,
                              application = "BSK trading",
                              token = self.token,
                              datetime_format = self.datetime_format)
            print('Oanda API context created.')
            return ctx
        except:
            print('There is a problem with the creation of the Oanda API context.')

    def create_streaming_context(self):
        """
        Initialize a streaming API context based on the configuration instance
        """
        try:
            ctx = v20.Context(self.streaming_hostname,
                              self.port,
                              self.ssl,
                              application = "BSK trading",
                              token = self.token,
                              datetime_format = self.datetime_format)
            print('Oanda Streaming API context created.')
            return ctx
        except:
            print('There is a problem with the creation of the Oanda Streaming API.')

    def validate(self):
        """
        Ensure configuration is valid
        """
        errors = []

        if self.hostname is None:
            errors.append("hostname")
        if self.streaming_hostname is None:
            errors.append("streaming hostname")
        if self.port is None:
            errors.append("port")
        if self.ssl is None:
            errors.append("ssl")
        if self.username is None:
            errors.append("username")
        if self.token is None:
            errors.append("token")
        if self.accounts is None:
            errors.append("account")
        if self.active_account is None:
            errors.append("account")
        if self.datetime_format is None:
            errors.append("datetime_format")

        if len(errors) > 0:
            print('Configuration file has error in:')
            for e in errors:
                print('    - '+e)
        else:
            print('Configuration file is OK.')


class SqlEngine():
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

    def load_configuration(self, path = CONFIG):
        """
        Load Yaml file with configuration from path
        
        Args: 
            path: path to yaml configuration file
        """
        self.path = path
        print('Loading from {}'.format(self.path))

        with open(os.path.expanduser(self.path), 'r') as stream:
            try:
                # Here must specify SQL section of the Yaml file
                y = yaml.load(stream)['sql']

                self.dialect = y.get('dialect', self.dialect)
                self.connector = y.get('connector', self.connector)
                self.server = y.get('server', self.server)
                self.password = y.get('password', self.password)
                self.port = y.get('port', self.port)
                self.user = y.get('user', self.user)
                self.dbname = y.get('dbname', self.dbname)
                self.echo = y.get('echo', self.echo)

                self.validate()

            except yaml.YAMLError as exc:
                print(exc)

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
                'There is a problem with the engine creation - Check configuration or '
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


if __name__ == '__main__':
    engine = SqlEngine()
    engine.load_configuration(CONFIG)
    engine.create_engine()

    api_ctx = OandaContext('demo')
    api_ctx.load_configuration(CONFIG)
    api_ctx.create_context()
