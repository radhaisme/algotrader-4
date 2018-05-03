# ALGOTRADER

## WHAT IS IT?:

Event driven engine for analysis and trading of financial assets.

Written in Python 3, work in progress.


## CONFIG FILE STRUCTURE

Configuration is keep in a **Yaml** file stored locally, the file is pointed to an 
environmental variable called **TRADE_CONF**. See: /common/config. 

The config file has a structure like:

```yaml
default:
    # Select type of connection to broker: "live"/"demo"
    oanda_conn: demo

oanda_demo:
    hostname: api-fxpractice.oanda.com
    streaming_hostname: stream-fxpractice.oanda.com
    port: 443
    ssl: true
    token: your_demo_token
    username: your_demo_user
    accounts:
    - xxx-xxx-xxxxxx-xxx
    - yyy-yyy-yyyyyy-yyy
    active_account: xxx-xxx-xxxxxx-xxx
 
oanda_live:
    hostname: api-fxtrade.oanda.com
    streaming_hostname: stream-fxtrade.oanda.com
    port: 443
    ssl: true
    token: your_live_token
    username: your_live_user
    accounts:
    - xxx-xxx-xxxxxx-xxx
    - yyy-yyy-yyyyyy-yyy
    active_account: xxx-xxx-xxxxxx-xxx

quandl:
    token: your_quandl_token
    
influx:
    # Settings here depends of your database engine
    host: 127.0.0.1
    port: 8086
    user: your_user
    password: your_password
    database: securities_master


```


