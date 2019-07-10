# -*- coding:shift-jis -*-

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')
import os

import ctypes

import datetime as dt
import quandl as ql
import pandas as pd
import csv

from bs4 import BeautifulSoup

import IoUtility as iou
import WebUtility as weu
import DataTypeUtility as dtu

import time
import threading

def make_datetime_yyyymmdd(_file):

    #for file in iou.getfiles(_folder):  # 本当は iou.get_writable_files():
    #    file = _folder + file
    #df = pd.read_csv(_file, names=['date','time','P','V','PE','non','B','BE','BS','A','AE','AS'])
    reader = pd.read_csv(_file, names=['date','time','P','V','PE','non','B','BE','BS','A','AE','AS'], chunksize=2000000)
    # http://stackoverflow.com/questions/19738169/convert-column-of-date-objects-in-pandas-dataframe-to-strings
    # http://stackoverflow.com/questions/6288892/convert-datetime-format
    for df in reader:
        # if date format is MM-DD-YYYY, then make it to YYYY-MM-DD
        df['date'] = df['date'].apply(lambda x : dt.datetime.strptime(x,'%m-%d-%Y').strftime('%Y-%m-%d') if (x[-5]=='-') & (x[-8]=='-') else x)
        df['date'] = df['date'].apply(lambda x : dt.datetime.strptime(x,'%m/%d/%Y').strftime('%Y-%m-%d') if (x[-5]=='/') & (x[-8]=='/') else x)
        #print(df['date'])
        iou.removeReadOnly(_file)
        iou.write_csv(df, _file, _isHeader=False)
        iou.setReadOnly(_file)

def divide_to_day2day(_folder, _files, _open, _close):

    for _file in _files:

        _file = _folder + _file

        print('Dividing {0} into subfiles'.format(_file))
        # http://stackoverflow.com/questions/34091877/how-to-add-header-row-to-a-pandas-dataframe
        # http://stackoverflow.com/questions/17465045/can-pandas-automatically-recognize-dates
        # http://stackoverflow.com/questions/21414885/parsing-datestring-in-pandas
        parser = lambda _date : pd.datetime.strptime(_date, '%Y-%m-%d %H:%M:%S') # run-time error arises when the datetime format doesn't fit with the one specified here
        try:
            #df = pd.read_csv(_file, names=['date','time','P','V','PE','flag','B','BE','BS','A','AE','AS'], parse_dates={'datetime': ['date', 'time']}, date_parser=parser)
            reader = pd.read_csv(_file, names=['date','time','P','V','PE','flag','B','BE','BS','A','AE','AS'], parse_dates={'datetime': ['date', 'time']}, date_parser=parser, chunksize=2000000)
            df_list = [r for r in reader]
            #for r in reader:
            #    print(r)
        except:
            make_datetime_yyyymmdd(_file)
            divide_to_day2day(_folder, _file, _open, _close)
            return -1

        # while non-empty list
        # http://stackoverflow.com/questions/19828822/how-to-check-whether-a-pandas-dataframe-is-empty
        reader_index = 0
        # read in partial data/
        df = df_list[reader_index]
        while(True):

            #print(df['datetime'].head(10))

            # determine the open & close time of the session
            target_open_date = df['datetime'][0].date()
            # add days (date operation)
            # http://stackoverflow.com/questions/6871016/adding-5-days-to-a-date-in-python
            target_close_date = (df['datetime'][0]+dt.timedelta(days=1)).date() if int(_close.split(':')[0]) < 10 else df['datetime'][0].date()

            # read in more data at least up to the session close time
            while df['datetime'].tolist()[-1] < dtu.str_to_datetime('{0} {1}'.format(target_close_date,_close)):
                reader_index += 1
                if reader_index < len(df_list):
                    tmp_df = df_list[reader_index]
                    df = df.append(tmp_df)
                else:
                    break
            #print(df['datetime'])

            print('{0} : {1} {2} ～ {3} {4}'.format(_file,target_open_date,_open,target_close_date,_close))
            # http://stackoverflow.com/questions/29370057/select-dataframe-rows-between-two-dateshttp://stackoverflow.com/questions/29370057/select-dataframe-rows-between-two-dates    
            day_df = df[(df['datetime'] >= dtu.str_to_datetime('{0} {1}'.format(target_open_date,_open))) & (df['datetime'] <= dtu.str_to_datetime('{0} {1}'.format(target_close_date,_close)))]        
            # http://stackoverflow.com/questions/24813673/split-datetime-column-into-a-date-and-time-python
            # warning : http://qiita.com/gymnstcs/items/c6e450303393d4afa343
            day_df['date'] = day_df['datetime'].apply(lambda x:x.date())
            day_df['time'] = day_df['datetime'].apply(lambda x:x.time())
            # rearange the columns order
            day_df = day_df[['date','time','P','V','PE','flag','B','BE','BS','A','AE','AS']]
            #print(day_df)
            newfile = _file.replace('_0.csv','_0_{0}.csv'.format(target_open_date))
            if not iou.file_exist(newfile):
                iou.write_csv(day_df,newfile,_isHeader=False)
                iou.setReadOnly(newfile)

            # ---------------------------------------------------------------------------------------------------------------
            #
            # preparing to divide the remaining tick data
            # get the first line where the following day tick start
            # http://stackoverflow.com/questions/21800169/python-pandas-get-index-of-rows-which-column-matches-certain-value
            print('datetime[0] = ','',df['datetime'][0])
            next_open_date = (df['datetime'][0]+dt.timedelta(days=1)).date()
            remaining_df = df[df['datetime'] >= dtu.str_to_datetime('{0} {1}'.format(next_open_date,_open))]
            if(not remaining_df.empty):             # this df has more data to be processed
                next_day_line = remaining_df.index[0]
                df = df.ix[next_day_line:]
                # renumbering the index so that line 1 starts from 0
                # http://stackoverflow.com/questions/20490274/how-to-reset-index-in-a-pandas-data-frame
                df = df.reset_index(drop=True)
                print('== 1 ==')
                print(len(df))
            else:                                   # this df has no more data to be prcedd
                print('== 2 ==')
                if reader_index >= len(df_list):      # no more df waiting in the df_list to be processed
                    print('== 2.1 ==')
                    break
                else:                                 #    more df waiting in the df_list to be processed
                    print('== 2.2 ==')
                    reader_index += 1
                    df = df_list[reader_index]
                    print(df)

            # create a empty pandas dataframe
            # http://blog.mwsoft.jp/article/173308699.html
            #df = pd.DataFrame(columns=['date','time','P','V','PE','flag','B','BE','BS','A','AE','AS'])
            
            #print(df)                        
        
        df = None        
        df_list.clear()
        print(reader)
        # finished dividing into day-to-day tick files
        # let's remove the original file
        iou.removeReadOnly(_file)
        #iou.delete_file(_file)

# divide tick files with multiple days' tick into single day's tick
if __name__ == '__main__':

    exchs = [
             #['FUTURES','--','--'],
             #['AMEX','07:00:00','06:59:59'],
             ['NYSE','07:00:00','06:59:59'],
             #['TSE','09:00:00','16:00:00'],
             #['EQUIDUCT','16:00:00','0:30:00'],
             #['ASX','09:00:00','16:00:00'],
             #['SHG','09:00:00','15:30:00'],
             #['ETF','17:00:00','02:00:00'],
             #['U.S.OTC','23:30:00','05:00:00'],
             #['Bombay','12:35:00','19:30:00'],
             #['NASDAQ','09:00:00','08:59:59'],
             #['ARCA','09:00:00','16:00:00'],
             #['SP500','09:00:00','16:00:00'],
             #['Stuttgart','08:00:00','07:30:00'],
            ]

    exchs = pd.DataFrame(exchs, columns=['exchange','open','close'])
    #print(exchs)

    i = 0
    m_thread = []
    for index,exch in exchs.iterrows():
        folder = 'F:/QCollector Expert For eSignal/{0}/'.format(exch['exchange'])
        files = iou.getfiles(folder,'_0.csv')
        t = threading.Thread(target=divide_to_day2day, args=[folder,files,'07:00:00','06:59:59'],name="thread{0}".format(i))
        m_thread.append(t)
        t.start()
        i += 1

    print('')
    for worker in m_thread:
        print('joining {0}'.format(worker.getName()))
        worker.join()

    print('done!')

    ## http://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe
    #for index,exch in exchs.iterrows():
    #    folder = 'F:/QCollector Expert For eSignal/{0}/'.format(exch['exchange'])        
    #    for file in iou.getfiles(folder,'_0.csv'):
    #        file = folder + file
    #        divide_to_day2day(file,exch['open'],exch['close'])