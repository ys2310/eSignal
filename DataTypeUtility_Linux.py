class DataConversion(object):
    """description of class"""

import datetime as dt
from numba import jit

def parse_any_date_str_to_iso_date(_datestr):
    # 参考 : http://stackoverflow.com/questions/969285/how-do-i-translate-a-iso-8601-datetime-string-into-a-python-datetime-object
    import dateutil.parser
    parsed_date = dateutil.parser.parse(_datestr)
    return datetime_to_str(parsed_date)

#def str_to_datetime(str, _str_format='%Y-%m-%d %H:%M:%S.%f'):
def str_to_datetime(str, _str_format='%Y-%m-%d %H:%M:%S'):
    return dt.datetime.strptime(str, _str_format)

def str_to_date(_datetime, _str_format='%Y-%m-%d'):
    tmp = dt.datetime.strptime(_datetime, _str_format).date()
    return dt.date(tmp.year, tmp.month, tmp.day)

def datetime_to_str(_datetime, _str_format='%Y-%m-%d %H:%M:%S.%f'):
    return _datetime.strftime(_str_format)

def date_to_str(_date, _str_format='%Y-%m-%d'):
    return _date.strftime(_str_format)

def timestamp_to_datetime(_timestamp):
    if isinstance(_timestamp, dt.datetime):
        return _timestamp
    return _timestamp.to_pydatetime()

def str_to_timestamp(_datetime, _str_format='%Y-%m-%d %H:%M:%S.%f'):
    import time    
    return dt.datetime.strptime(_datetime, _str_format)

def numpy_to_native(var):
    return var.item()

def list_to_2D_array(_data, _row_num, _col_num):
    import numpy
    return numpy.reshape(_data, (_row_num, _col_num))