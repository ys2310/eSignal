import sys
sys.path.append('C:/Users/steve/Documents/Visual Studio 2015/Projects/PythonStats/x64/Release/')
sys.path.append('C:/Users/steve/Documents/Visual Studio 2015/Projects/PythonStats/PythonUtility/')

import pandas as pd
import datetime as dt
import doctest
import hoge
import DataTypeUtility as dt_util
import GeneralUtility as gn_util
import DatetimeUtility as dtm_util
import IoUtility as io_util
import StringUtility as str_util

print(hoge.greet())
print(hoge.add(4,5))

def compare_time(dt1, dt2):
    if dtm_util.diff(dt1, dt2) > 10:
        print("abnormal data on datetime found @ ", dt1, dt2)
    else:
        pass

def read_csv_eSignal(_file, _num_cols):
    if _num_cols == 13:
        #mc = pn_util.measure_clock(lambda : pd.read_csv(file, encoding='Shift-JIS', header=None))
        mc = gn_util.measure_clock(lambda : pd.read_csv(_file, encoding='Shift-JIS', header=None, names=('Date','Time','TP','TQ','TE','Dummy','BP','BE','BQ','AP','AE','AQ','BxA'), dtype={'Date':object, 'Time':object ,'TP':float, 'TQ':float, 'TE':str, 'Dummy':str, 'BP':float, 'BE':str, 'BQ':float, 'AP':float, 'AE':str, 'AQ':float, 'BxA':str}))            
    else:
        mc = gn_util.measure_clock(lambda : pd.read_csv(_file, encoding='Shift-JIS', header=None, names=('Date','Time','TP','TQ','TE','Dummy','BP','BE','BQ','AP','AE','AQ'), dtype={'Date':object, 'Time':object ,'TP':float, 'TQ':float, 'TE':str, 'Dummy':str, 'BP':float, 'BE':str, 'BQ':float, 'AP':float, 'AE':str, 'AQ':float}))    
    return mc

base_dir = 'F:/'
folders = io_util.getfolders(base_dir)
folders = str_util.list_not_contains_str(folders, ["_", "ARCA", 'AsiaFutures'])

csv_func = lambda file, num_cols : read_csv_eSignal(file, num_cols)
integrity_check_func1 = lambda data, colname, lag : map(compare_time, data[colname], data[colname][lag:]), None, 'Date', 1
#def integrity_check_func1(data, colname = 'Date', lag=1):
#    map(compare_time, data[colname], data[colname][lag:])
    #if dtm_util.diff(data[colname], data[colname][lag:]) > 10:
    #    print("abnormal data on datetime found @ ", data[colname], data[colname][lag:]) 

io_util.read_files_in_folders_and_dosomething(base_dir, folders, csv_func, [integrity_check_func1])
