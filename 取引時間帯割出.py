# -*- coding:shift-jis -*-
import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')

import IoUtility as iou
import OsUtility as osu
import DatetimeUtility as dmu

import threading

import time
import datetime
from subprocess import call

import pandas as pd

def find_exchange_hours(_path, _folder, _worker):

    folder = _path + _folder
    files = iou.getfiles(folder)

    # find the file with the max size in each folder
    maxsize = 0
    maxfile = ''
    for file in files:
        file = folder + '/' + file
        filesize = osu.get_file_size(file)
        if filesize > maxsize:
            maxsize = filesize
            maxfile = file
  
    #maxfile = 'F:/QCollector Expert For eSignal/TSE/8065-TSE_0.csv'
    print('worker {0} : maxfile for {1} is {2} with {3}'.format(_worker,folder,maxfile,maxsize))
    parser = lambda _date : pd.datetime.strptime(_date, '%Y-%m-%d %H:%M:%S')
    df = pd.read_csv(maxfile, names=['date','time','P','V','PE','flags','B','BE','BS','A','AE','AS'], header=None, parse_dates={'datetime': ['date','time']}, date_parser=parser)
    # datetime を index として設定しo、日付型Indexお (DatetimeIndex) に変換
    df = df.set_index(['datetime'])
    df.index = pd.to_datetime(df.index)
    #print(df.head(10))
    #print(df.index)
    # resampling by the datetime column
    # http://stackoverflow.com/questions/20048200/averaging-every-five-minutes-data-as-one-datapoint-in-pandas-dataframe
    df = df.resample('5Min').count()
    # column 集計
    df['Total'] = df.sum(axis=1)
    #print(df)
    # if the 2nd row from the last row is 0, drop the last row bcause this is a dummy record
    if df['Total'].ix[-2,:]==0:
        df = df.ix[:-1]
    # move up from the bottom up to till [Total] is not 0
    # reversed range step -1
    # http://d.hatena.ne.jp/emergent/20101031/1288532125
    for i in range(len(df))[::-1]:
        if df['Total'][i]==0:
            #print(i,df['Total'][i])
            continue
        else:                
            close_time = df.index[i]
            break

    # 30分単位で後続の一番近い時間帯を close_time とする
    close_hour = close_time.hour
    close_minute = close_time.minute
    if close_minute > 30:
        close_hour += 1
        close_minute = 0
    elif close_minute > 0:
        close_minute = 30
    close_time = close_time.replace(hour=close_hour, minute=close_minute)
    open_time = df.index[0]
    print(folder,' : ',open_time,'～',close_time)
    #print(df)
    #print('')

# 取引所取引時間自動割出
if __name__ == "__main__":
    
    folders = ['FUTURES']#,'U.S.OTC','Stuttgart','SP500','DME','Bombay','TSE','LME','ETF','ARCA','ASX','EQUIDUCT','NASDAQ','SHG']
    path = 'F:/QCollector Expert For eSignal/'

    m_thread = []
    for i,folder in enumerate(folders):
        t = threading.Thread(target=find_exchange_hours, args=[path,folder,i],name="thread{0}".format(i))
        m_thread.append(t)
        t.start()

    print('')
    for worker in m_thread:
        print('joining {0}'.format(worker.getName()))
        worker.join()

    print('done!')