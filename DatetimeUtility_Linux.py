# -*- coding:shift-jis -*-

# 2016-04-24
#from numba import jit

import pandas
import datetime
from datetime import date
import DataTypeUtility as dpu

def get_currnet_datetime_str(_str_format='%Y-%m-%d %H:%M:%S'):
    return dpu.datetime_to_str(datetime.datetime.now(), _str_format)

def is_weekday(_year, _month, _day):
    youbi = date(_year if isinstance(_year, int) else int(_year), 
                 _month if isinstance(_month, int) else int(_month), 
                 _day if isinstance(_day, int) else int(_day)).weekday()
    if youbi < 5:
        return True
    else: # 5 = Saturday, 6 = Sunday
        return False    

def mm_dd_yyyy_to_yyyy_mm_dd(_dt):
    # http://stackoverflow.com/questions/502726/converting-date-between-dd-mm-yyyy-and-yyyy-mm-dd
    return datetime.datetime.strptime(_dt, "%m-%d-%Y").strftime("%Y-%m-%d")

def diff(_dt1, _dt2, _unit='days'):
    if type(_dt1) is str:
        dt1 = dt_util.str_to_date(_dt1)
    elif type(_dt1) is datetime.date:
        dt1 = _dt1        
    elif type(_dt1) is pandas.tslib.Timestamp:
        dt1 = _dt1.to_pydatetime()
    elif type(_dt1) is datetime.datetime:
        dt1 = _dt1
    else:
        return 'Wrong data tpye ! (type of ' + str(type(_dt1)) + ' was passed)'

    if type(_dt2) is str:
        dt2 = dt_util.str_to_date(_dt2)
    elif type(_dt2) is datetime.date:
        dt2 = _dt2
    elif type(_dt2) is pandas.tslib.Timestamp:
        dt2 = _dt2.to_pydatetime()
    elif type(_dt2) is datetime.datetime:
        dt2 = _dt2
    else:
        return 'Wrong data tpye ! (type of ' + str(type(_dt2)) + ' was passed)'

    if _unit == 'years':
        return (dt2 - dt1).years
    elif _unit == 'months':
        return (dt2 - dt1).months
    elif _unit == 'days':
        return (dt2 - dt1).days
    elif _unit == 'hours':
        return (dt2 - dt1).hours
    elif _unit == 'minutes':
        return (dt2 - dt1).minutes
    elif _unit == 'seconds':
        return (dt2 - dt1).seconds
    else:    
        return (dt2 - dt1).milliseconds

def datetime_add(_datetime, _length):
    if type(_datetime) is str:
        a = dpu.str_to_datetime(_datetime)
    else:
        a = _datetime
    b = a + datetime.timedelta(days=_length)
    return dpu.datetime_to_str(b)


def date_add(_date, _length):
    if type(_date) is str:
        a = dpu.str_to_date(_datetime)
    else:
        a = _date
    b = a + datetime.timedelta(days=_length)
    return dpu.date_to_str(b)

# 
def timeadd(_datetime_str, _length):        
    a = tp_util.str_to_datetime(_datetime_str)
    b = a + datetime.timedelta(seconds=_length)
    return tp_util.datetime_to_str(b)
