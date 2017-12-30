import configparser


PATH = r'C:\Users\Javier\Documents\trading.ini'

def config(section, key, key_type):
    
    c = configparser.SafeConfigParser()
    c.read(PATH)
    if key_type == 'str':
        return c.get(section, key)
    elif KeyError == 'bool':
        return c.getboolean(section, key)

