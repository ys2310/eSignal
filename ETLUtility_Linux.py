# -*- coding:shift-jis -*-

import sys
#sys.path.append('../x64/Debug/')
sys.path.append('../x64/Release/')
sys.path.append('../PythonUtility/')

import datetime as dt
import pandas as pd
import numpy as np
import pyodbc

import IoUtility as iou

def df_to_pivot(_df, _index_list, _column_list, _agg_field, _partial_magrin=True, _numpy_func=np.sum):
    # http://pandas.pydata.org/pandas-docs/stable/reshaping.html
    return pd.pivot_table(_df, values=_agg_field, index=_index_list, columns=_column_list, margins=_partial_magrin, aggfunc=_numpy_func) # 欠損値

def df_to_crostab():
    a

if __name__ == '__main__':

    # test 
    df = iou.read_csv('G:\HiveStat\daily_hourly_act_count\daily_hourly_eSignal.AMEX_orc_2016-12-01_2016-12-01.csv', ['Cat','UpDn','Ticker','Day','Hour','Cnt'])
    pivot = df_to_pivot(df, ['Ticker'], ['Hour','Cat','UpDn'], 'Cnt')
    pivot = pivot.replace(np.nan, '')
    print(pivot)
    iou.write_csv(pivot, 'G:\HiveStat\_pivot\daily_hourly_eSignal.AMEX_orc_2016-12-01_2016-12-01.csv', _isIndex=True)
    