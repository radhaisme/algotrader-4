import datetime
import rfc3339  
import iso8601


def price_to_string(price):
    return "{} ({}) {}/{}".format(
        price.instrument,
        price.time,
        price.bids[0].price,
        price.asks[0].price
    )

def heartbeat_to_string(heartbeat):
    return "HEARTBEAT ({})".format(
        heartbeat.time
    )


def price_to_string2(price):
    my_time = get_date_object(price.time)
    #my_time = get_date_string(my_time)
    return type(my_time)
    
def get_date_string(price):
    return rfc3339.rfc3339(price)

def get_date_object(date_string):
    return iso8601.parse_date(date_string)



