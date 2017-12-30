import configparser


PATH = r'C:\Users\Javier\Documents\trading.ini'

def config(section, key, key_type):
    
    c = configparser.SafeConfigParser()
    c.read(PATH)
    if key_type == 'str':
        return c.get(section, key)
    elif KeyError == 'bool':
        return c.getboolean(section, key)


def mysql_config():
    return {'dialect':config(section='MYSQL', key='dialect', key_type='str'),
            'conector':config(section='MYSQL', key='conector', key_type='str'),
            'server':config(section='MYSQL', key='server', key_type='str'),
            'port':config(section='MYSQL', key='port', key_type='str'),
            'user':config(section='MYSQL', key='user', key_type='str'),
            'psw':config(section='MYSQL', key='password', key_type='str'),
            'dbname':config(section='MYSQL', key='dbname', key_type='str'),
            'echo':config(section='MYSQL', key='echo', key_type='bool')}