import configparser
import yaml
import os
from pprint import pprint


CONFIG = os.environ['TRADE_CONF']


def read_yaml_conf(conf_file, section):
    """
    Read configuration file and return dictionary for specific section
    """
    ans = yaml.load(open(conf_file))
    return ans[section]


def mysql_config():
    config = read_yaml_conf(CONFIG, 'sql')
    
    return {'dialect':config['dialect'],
            'conector':config['conector'],
            'server':config['server'],
            'port':config['port'],
            'user':config['user'],
            'password':config['password'],
            'dbname':config['dbname'],
            'echo':config['echo']}
    
def oanda_config():
    return {'oanda_demo_api_key':config(section='OANDA', key='oanda_demo_api_key', key_type='str'),
            'oanda_live_api_key':config(section='OANDA', key='oanda_live_api_key', key_type='str'),
            'url_rest_demo':config(section='OANDA', key='url_rest_demo', key_type='str'),
            'url_rest_live':config(section='OANDA', key='url_rest_live', key_type='str'),
            'url_stream_demo':config(section='OANDA', key='url_stream_demo', key_type='str'),
            'url_stream_live':config(section='OANDA', key='url_stream_live', key_type='str'),
            'alpha_demo_v20':config(section='OANDA', key='alpha_demo_v20', key_type='str')}
    
    
if __name__ == '__main__':


    print(mysql_config())
    
    
    