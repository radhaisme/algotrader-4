# -*- coding: utf-8 -*-
"""
Created on 3 Jan 2018

@author: Javier
"""
import yaml
import os
import v20


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

    def load_configuration(self, path):
        """
        Load Yaml file with configuration from path

        Args:
            path: path to yaml configuration file
        """
        self.path = path
        print('Loading from {}'.format(self.path))
        with open(os.path.expanduser(self.path), 'r') as stream:
            try:
                # Here must specify demo or live oanda_common section of the Yaml file
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

