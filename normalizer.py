import ipaddress
import datetime

from dateutil.parser import parse
from pyspark.sql.functions import udf


@udf
def normalize_string(country):
    return country.capitalize()


@udf
def validate_ip_address(ip_address):
    try:
        if str(ipaddress.IPv4Address(ip_address)):
            return str(ipaddress.IPv4Address(ip_address))
    except Exception as ex:
        # print("Invalid IP address: {} - Exception: {}".format((ip_address if ip_address else ""), str(ex)))
        return None


@udf
def validate_date(date):
    try:
        date_parsed = parse(date)
        normalized_date = datetime.datetime.strftime(date_parsed, '%d/%m/%Y')
        return normalized_date
    except Exception as ex:
        # print("Invalid Date format: {} - Exception: {}".format((date if date else ""), str(ex)))
        return None
