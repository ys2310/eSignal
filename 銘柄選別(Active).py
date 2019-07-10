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

import time
from subprocess import call

import pandas as pd

# 6ヶ月に1回の頻度で更新を行う。
if __name__ == "__main__":
    
    #portfolios = ['ARCA','ASX','BALTIC','DME','EQUIDUCT','ETF','FUNDS','HKG','LME','MarketStatistics','NASDAQ','SHG','SP500','TSE'] # SouthEastAsiaStock, AsiaFutures, DeutschBorse, LSE, EuroNext
    # 'BALTIC'は週次データも多いので全銘柄採用
    #portfolios = ['Swiss','Luxenberg','OMX','U.S.OTC','Stuttgart','SP500','DME','Bombay','TSE','LME','ETF','ARCA','ASX','EQUIDUCT','NASDAQ','SHG']
    interval = 0
    
    portfolio = sys.argv[1]    
    #for portfolio in portfolios:

    print('Tick銘柄選別 for portfolio {0}'.format(portfolio))       

    files = []
    # for each latest saved tick data
    for file in iou.getfiles('F:/QCollector Expert For eSignal/{0}'.format(portfolio), '.csv'):
        csv_name = file
        file = 'F:/QCollector Expert For eSignal/' + portfolio + '/' + file
        # drop inactive tickers (file size less than 2kb)
        size = osu.get_file_size(file)
        if size > 2000 or csv_name[0]=='$':
            files.append(csv_name.replace('_0.csv',''))
        #try:
        #    date = pd.read_csv(file, header=None).iloc[0].ix[0]
        #    iou.setReadOnly(file)
        #    os.rename(file, file.replace('.csv','_{0}.csv'.format(date.replace('/','-'))))
        #except:
        #    print('skipped {0}'.format(file))
    # get unique tickers
    files = [x.split('_0_')[0] for x in files]
    #files = list(set(files))

    # ticker list file
    csv = pd.read_csv('F:/QCollector Expert For eSignal/Symbol {0}.csv'.format(portfolio), header=None)    
    # pandas series to list
    tickers = csv.ix[:,0].tolist()  #１列目    
    #print(tickers)
    
    # set operation 積集合 (共通部分)
    # http://python.civic-apps.com/set/
    tick_active_tickers = sorted(list(set(files).intersection(tickers)))
    pd.DataFrame(tick_active_tickers).to_csv('F:/QCollector Expert For eSignal/tick_symbols_{0}.csv'.format(portfolio), index=False, header=False)

    print('')