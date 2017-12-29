import configparser


PATH = r'C:\Users\Javier\Documents\trading.ini'

def config(section, key):
    
    c = configparser.SafeConfigParser()
    c.read(PATH)
    return c.get(section, key)


