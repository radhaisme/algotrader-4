import yaml
import os
from pprint import pprint


CONFIG = os.environ['TRADE_CONF']
 

def read_yaml_conf(conf_file, section):
    """
    Read configuration file and return dictionary for specific section
    """
    with open(conf_file, 'r') as stream:
        try:
            ans = yaml.load(stream)
            return ans[section]
        except yaml.YAMLError as exc:
            print(exc)
    


def mysql_config():
    """
    SQL engine configuration
    """
    config = read_yaml_conf(CONFIG, 'sql')
    
    return {'dialect':config['dialect'],
            'conector':config['conector'],
            'server':config['server'],
            'port':config['port'],
            'user':config['user'],
            'password':config['password'],
            'dbname':config['dbname'],
            'echo':config['echo']}


def oanda_config(connection_type):
    """
    OANDA trading engine configuration 
    """
    # What type of connection are we going to use?
    if connection_type == 'demo':
        my_type= 'oanda_demo'
    elif connection_type == 'live':
        my_type= 'oanda_live'
    else:
        print('Only "demo" or "live" connection types.\nProgram terminated.')
        quit()
        
        
    config = read_yaml_conf(CONFIG, my_type)
    
    return {'hostname':config['hostname'],
            'streaming_hostname':config['streaming_hostname'],
            'port':config['port'],
            'ssl':config['ssl'],
            'token':config['token'],
            'accounts':config['accounts'],
            'active_account':config['active_account']}
            
            
  
    
if __name__ == '__main__':
    
    config = oanda_config('live')
    pprint(config)

    
