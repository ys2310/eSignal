# -*- coding:shift-jis -*-

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')

import ctypes

import datetime as dt
import quandl as ql
import pandas as pd
import csv

import IoUtility as iou
import WebUtility as weu
import DataTypeUtility as dtu

import time

target_date = '2016-08-25'
hours_dict = dict({'TDG':['14:30:00','5:00:00']})

def remove_extra(_folder):
    for file in iou.get_writable_files(_folder):
        file = _folder + file
        # http://stackoverflow.com/questions/34091877/how-to-add-header-row-to-a-pandas-dataframe
        # http://stackoverflow.com/questions/17465045/can-pandas-automatically-recognize-dates
        parser = lambda _date : pd.datetime.strptime(_date, '%Y-%m-%d %H:%M:%S') #dtu.str_to_datetime(_date)
        df = pd.read_csv(file, names=['date','time','P','V','PE','flags','B','BE','BS','A','AE','AS'], parse_dates={'datetime': ['date', 'time']}, date_parser=parser)
        #print(df['datetime'].head(10))
        # http://stackoverflow.com/questions/29370057/select-dataframe-rows-between-two-dateshttp://stackoverflow.com/questions/29370057/select-dataframe-rows-between-two-dates    
        df = df[(df['datetime'] >= dtu.str_to_datetime('2016-08-24 14:30:00')) & (df['datetime'] <= dtu.str_to_datetime('2016-08-25 5:00:00'))]
        print(df)
        # overwrite the original file which contains extras
        iou.write_csv(df, file, _isHeader=False)

def make_datetime_yyyymmdd(_folder):
    for file in iou.get_writable_files(_folder):
        file = _folder + file
        df = pd.read_csv(file, names=['date','time','P','V','PE','non','B','BE','BS','A','AE','AS'])
        # http://stackoverflow.com/questions/19738169/convert-column-of-date-objects-in-pandas-dataframe-to-strings
        # http://stackoverflow.com/questions/6288892/convert-datetime-format
        # if date format is MM-DD-YYYY, then make it to YYYY-MM-DD
        df['date'] = df['date'].apply(lambda x : dt.datetime.strptime(x,'%m-%d-%Y').strftime('%Y-%m-%d') if (x[-5]=='-') & (x[-8]=='-') else x)
        print(df['date'])
        iou.write_csv(df, file, _isHeader=False)

def check_file_existance():
    target_date = dt.datetime.today().date
    files_to_check = ['C:/Users/P000505/Desktop/test1/00RC-TDG_0.csv', 
                      'C:/Users/P000505/Desktop/test2/00RC-TDG_0.csv']
    for file in files_to_check:
        print('{0} not found! for the date {1}'.format(file, target_date)) if iou.file_exist(file) else None

if __name__ == '__main__':
    # checking open public data's daily existance
    check_file_existance()
    # checking datetime format, datetime range
    folders = ['C:/Users/P000505/Desktop/test1/', 'C:/Users/P000505/Desktop/test2/']
    for folder in folders:
        print('Checking files integirity fo folder {0}'.format(folder))        
        make_datetime_yyyymmdd(folder)
        remove_extra(folder)
        print('')