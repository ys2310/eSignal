# -*- coding:shift-jis -*-

import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PythonUtility/')

import ctypes

import datetime as dt
import quandl as ql
import pandas as pd
import csv

#from bs4 import BeautifulSoup

import IoUtility as iou
import WebUtility as weu
import DataTypeUtility as dtu
import DataCollectionUtility as dcu

import time
import threading

def make_datetime_yyyymmdd(_file):

    #for file in iou.getfiles(_folder):  # 本当は iou.get_writable_files():
    #    file = _folder + file
    df = pd.read_csv(_file, names=['date','time','P','V','PE','non','B','BE','BS','A','AE','AS'])
    # http://stackoverflow.com/questions/19738169/convert-column-of-date-objects-in-pandas-dataframe-to-strings
    # http://stackoverflow.com/questions/6288892/convert-datetime-format
    # if date format is MM-DD-YYYY, then make it to YYYY-MM-DD
    df['date'] = df['date'].apply(lambda x : dt.datetime.strptime(x,'%m-%d-%Y').strftime('%Y-%m-%d') if (x[-5]=='-') & (x[-8]=='-') else x)
    df['date'] = df['date'].apply(lambda x : dt.datetime.strptime(x,'%m/%d/%Y').strftime('%Y-%m-%d') if (x[-5]=='/') & (x[-8]=='/') else x)
    #print(df['date'])
    iou.removeReadOnly(_file)
    iou.write_csv(df, _file, _isHeader=False)
    iou.setReadOnly(_file)

def divide_to_day2day(_folder, _files, _open, _close):
    
    for _file in _files:

        if "#OI" in _file:
            continue

        _file = _folder + _file

        print('Dividing {0} into subfiles'.format(_file))
        # http://stackoverflow.com/questions/34091877/how-to-add-header-row-to-a-pandas-dataframe
        # http://stackoverflow.com/questions/17465045/can-pandas-automatically-recognize-dates
        # http://stackoverflow.com/questions/21414885/parsing-datestring-in-pandas
        parser = lambda _date : pd.datetime.strptime(_date, '%Y-%m-%d %H:%M:%S') # run-time error arises when the datetime format doesn't fit with the one specified here
        try:
            df = pd.read_csv(_file, names=['date','time','P','V','PE','flag','B','BE','BS','A','AE','AS'], parse_dates={'datetime': ['date', 'time']}, date_parser=parser)
            #reader = pd.read_csv(_file, names=['date','time','P','V','PE','flag','B','BE','BS','A','AE','AS'], parse_dates={'datetime': ['date', 'time']}, date_parser=parser, chunksize=2000000) # gives TextFileReader, which is iterable with chunks of 2000000 rows
            #print(df)
        except:
            make_datetime_yyyymmdd(_file)
            divide_to_day2day(_folder, _file, _open, _close)
            return -1
        # while non-empty list
        # http://stackoverflow.com/questions/19828822/how-to-check-whether-a-pandas-dataframe-is-empty
        while(True):
            #print(df['datetime'].head(10))
            target_open_date = df['datetime'][0].date()
            # add days (date operation)
            # http://stackoverflow.com/questions/6871016/adding-5-days-to-a-date-in-python
            target_close_date = (df['datetime'][0]+dt.timedelta(days=1)).date() if int(_close.split(':')[0]) < 10 else df['datetime'][0].date()
            print('{0} : {1} {2} ～ {3} {4}'.format(_file,target_open_date,_open,target_close_date,_close))
            # http://stackoverflow.com/questions/29370057/select-dataframe-rows-between-two-dateshttp://stackoverflow.com/questions/29370057/select-dataframe-rows-between-two-dates    
            day_df = df[(df['datetime'] >= dtu.str_to_datetime('{0} {1}'.format(target_open_date,_open))) & (df['datetime'] <= dtu.str_to_datetime('{0} {1}'.format(target_close_date,_close)))]        
            # http://stackoverflow.com/questions/24813673/split-datetime-column-into-a-date-and-time-python
            day_df['date'] = day_df['datetime'].apply(lambda x:x.date())
            day_df['time'] = day_df['datetime'].apply(lambda x:x.time())
            # rearange the columns order
            day_df = day_df[['date','time','P','V','PE','flag','B','BE','BS','A','AE','AS']]
            #print(day_df)
            newfile = _file.replace('_0.csv','_0_{0}.csv'.format(target_open_date))
            if not iou.file_exist(newfile):
                iou.write_csv(day_df,newfile,_isHeader=False)
                iou.setReadOnly(newfile)
            # preparing to divide the remaining tick data
            # get the first line where the following day tick start
            # http://stackoverflow.com/questions/21800169/python-pandas-get-index-of-rows-which-column-matches-certain-value
            next_open_date = (df['datetime'][0]+dt.timedelta(days=1)).date()        
            remaining_df = df[df['datetime'] >= dtu.str_to_datetime('{0} {1}'.format(next_open_date,_open))]
            if(not remaining_df.empty):
                next_day_line = remaining_df.index[0]
            else:
                break
            df = df.ix[next_day_line:]
            # renumbering the index so that line 1 starts from 0
            # http://stackoverflow.com/questions/20490274/how-to-reset-index-in-a-pandas-data-frame
            df = df.reset_index(drop=True)
            #print(df)

        # finished dividing into day-to-day tick files
        # let's remove the original file        
        iou.removeReadOnly(_file)
        iou.delete_file(_file)

# divide tick files with multiple days' tick into single day's tick
if __name__ == '__main__':

    now = dt.datetime.now()
    hour = now.hour
    minute = now.minute

    #print("引数の総個数 = {0}\n".format(len(sys.argv)))
    #if len(sys.argv) != 2:
    #    print("引数を正しく指定してください！")

    #for i,x in enumerate(sys.argv):
    #    print("{0}番目の引数 = {1}\n".format(i, x))

    # parameter : 0=this.py 1=exchange, (2=is_preprocessing)
    param = sys.argv
    if len(param) >= 2:
        exchange = param[1]  #'ASX'
    elif len(param) <= 2:
        # assign parameter based on this process's current execution time
        # opening hours : https://en.wikipedia.org/wiki/List_of_stock_exchange_opening_times
        if hour==18 and minute >= 15 and minute <= 20:          # 9:00 JST to 11:30 JST, 12:30 JST to 15:00 JST (UTC+9)
            exchange = 'TSE'
        elif hour==18 and minute >= 30 and minute <= 35:        # 8:45 JST to 15:15 JST, 16:30 JST to  5:30 JST (Rubber : 16:30 JST to 19:00 JST)  (UTC+9)
            exchange = 'TOCOM'
        elif hour==19 and minute >= 10 and minute <= 15:        # 9:30 CST to 11:30 CST, 13:00 CST to 15:00 CST (UTC+8)
            exchange = 'SHG'
        elif hour==19 and minute >= 30 and minute <= 35:        # 10:00 AEST to 16:00 AES (UTC+10)
            exchange = 'ASX'
        elif hour==22 and minute >= 10 and minute <= 15:        # 09:15 IST to 15:30 IST (UTC+5.5)
            exchange = 'Bombay'
        elif hour==4 and minute >= 45 and minute <= 50:         # 09:00 CET to 17:35 CET (UTC+1)
            exchange = 'Luxenberg'
            exit()
        elif hour==6 and minute >= 0 and minute <= 5:           # PAN EUROPEAN EXCHANGE Where Retail Meets Institutions : 9:01 CET to 17:30 CET (UTC+1)
            exchange = 'EQUIDUCT'
            exit()
        elif hour==6 and minute >= 25 and minute <= 30:         # 10:00	EET to 18:30 EET (UTC+2)
            exchange = 'OMX'
            exit()
        elif hour==7 and minute >= 15 and minute <= 20:         # 9:00 GMT to 16:00 GMT (UTC+0)
            exchange = 'BALTIC'
            exit()
        elif hour==7 and minute >= 40 and minute <= 45:         # 08:00	GMT to 16:30 GMT (UTC+0)
            exchange = 'LME'
            exit()
        elif hour==9 and minute >= 45 and minute <= 50:         # Various
            exchange = 'FUTURES'
            exit()
        elif hour==10 and minute >= 15 and minute <= 20:        # 09:30 EST to 16:00 EST (UTC-5)
            exchange = 'AMEX'
            exit()
        elif hour==11 and minute >= 15 and minute <= 20:        # 09:30 EST to 16:00 EST (UTC-5)
            exchange = 'NASDAQ'
            exit()
        elif hour==12 and minute >= 15 and minute <= 20:        # 09:30 EST to 16:00 EST (UTC-5)
            exchange = 'SP500'
            exit()
        elif hour==14 and minute >= 10 and minute <= 15:        # 4:00 EST to 20:00 EST (UTC-5)
            exchange = 'ARCA'
            exit()
        elif hour==16 and minute >= 30 and minute <= 35:        # Daily
            exchange = 'FUNDS' #, 'MarketStatistics'
            exit()
        else:
            exchange = 'ASX' #exit()
    if len(param)==3:
        is_preprocessing = param[2]
    else:
        is_preprocessing = False

    msg = 'Lunching データ分割 {0} ... '.format(exchange)
    iou.console_title(msg + ' @ ' + str(now), 'shift-jis')

    import subprocess
    # 大容量ファイル(>1GB)は予め分割しておく
    win_shell = "python C:/Users/steve/Desktop/PySong2/eSignal/大容量ファイル事前分割.py {0}".format(exchange)
    print(win_shell,'\n') # to check what been passed to win_shell
    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

    # 以下ファイル分割作業
    exchs = [
             ['{0}'.format(exchange),'--','--'],
             #['{0}'.format(sys.argv[1]),'--','--'],
             #['FUTURES','--','--'],
             #['Luxenberg','07:00:00','06:59:59'],
             #['AMEX','07:00:00','06:59:59'],
             #['NYSE','07:00:00','06:59:59'],
             #['TSE','09:00:00','16:00:00'],
             #['EQUIDUCT','16:00:00','0:30:00'],
             #['ASX','09:00:00','16:00:00'],
             #['SHG','09:00:00','15:30:00'],
             #['ETF','17:00:00','02:00:00'],
             #['U.S.OTC','23:30:00','05:00:00'],
             #['Bombay','07:00:00','06:59:59'],
             #['NASDAQ','09:00:00','08:59:59'],
             #['ARCA','07:00:00','06:59:59'],
             #['SP500','09:00:00','16:00:00'],
             #['Stuttgart','08:00:00','07:30:00'],
            ]

    exchs = pd.DataFrame(exchs, columns=['exchange','open','close'])
    #print(exchs)

    i = 0
    m_thread = []
    for index,exch in exchs.iterrows(): # a stopiteration was not handled exception will be thrown and just ignore it : http://qiita.com/gyu-don/items/0f38dfb48fc7dabbb2ae

        now = dt.datetime.now()
        msg = 'データ分割中 - with exchange {0}'.format(exch['exchange'])
        iou.console_title(msg + ' ' + str(now), 'shift-jis')

        folder = 'G:/QCollector Expert For eSignal/{0}/'.format(exch['exchange'])
        files = iou.getfiles(folder,'_0.csv')
        files1,files2 = dcu.split_list_in_half(files, True)
        if files1:  # check the existance of files
            t1 = threading.Thread(target=divide_to_day2day, args=[folder,files1,'07:00:00','06:59:59'],name="thread{0}.1".format(i))
            m_thread.append(t1)
            t1.start()
            i += 1
        if files2:  # check the existance of files
            t2 = threading.Thread(target=divide_to_day2day, args=[folder,files2,'07:00:00','06:59:59'],name="thread{0}.2".format(i))
            m_thread.append(t2)
            t2.start()
            i += 1

    print('')
    for worker in m_thread:
        print('joining {0}'.format(worker.getName()))   
        worker.join()

    if not is_preprocessing:

        # Dailyに分割されたticksを統計処理用に加工
        import subprocess
        win_shell = "python C:/Users/steve/Desktop/PySong2/eSignal/データ加工.py {0}".format(exchange)
        print(win_shell,'\n') # to check what been passed to win_shell
        cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
        returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

        # Hiveにデータ転送し、集計結果をWinローカルに保存
        import subprocess
        win_shell = "python C:/Users/steve/Desktop/PySong2/PyHadoop/PyInjection.py 2016-11-01 2017-02-28 {0}".format(exchange)
        print(win_shell,'\n') # to check what been passed to win_shell
        cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
        returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

    print('done!')

    ## http://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe
    #for index,exch in exchs.iterrows():
    #    folder = 'F:/QCollector Expert For eSignal/{0}/'.format(exch['exchange'])        
    #    for file in iou.getfiles(folder,'_0.csv'):
    #        file = folder + file
    #        divide_to_day2day(file,exch['open'],exch['close'])