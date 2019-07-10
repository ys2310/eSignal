# -*- coding:shift-jis -*-
import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')

import ctypes

import os
import mmap
import datetime
import quandl as ql
import pandas as pd
import csv

import IoUtility as iou
import OsUtility as osu
import DataTypeUtility as dpu
import DatetimeUtility as dtu
import WebUtility as wbu

import time

def get_open_data_folder_list(_list_file=''):
    return list(pd.read_csv(_list_file).ix[:,0])

def check_open_data_integrity():
    op_list = get_open_data_folder_list('')
    for x in op_list:
        if not file_exist(x):
            print('Integrity check failed at : {0}'.format(x))

def check_es_daily_data_existance(_path, _target_exchange, _tickers, _target_date, _log_file):
    
    #osu.delete_file(_log_file)
    open(_log_file, "w")
    for x in _tickers:
        ticker = x + '_0_{0}.csv'.format(_target_date)
        file = _path + '/' + ticker
        file = file.replace('//','/')
        zipped_file = file.replace('.csv','.zip')
        if not (iou.file_exist(file) or iou.file_exist(zipped_file)):
            # stdout
            print('<--! {0} was not found. --!>'.format(file))
            # write it to the log
            with open(_log_file, "a") as f:                
                f.write("{0},not exists\n".format(ticker))
            # inform it to admin via an email
            mail_title = ''
            mail_body = ''
            #wbu.send_gmail(mail_title, mail_body)

def check_es_field_integrity(_path, _target_exchange, _target_date, _log_file):
    
    for file in iou.getfiles(_path,'_0_{0}.csv'.format(_target_date)):
        file = _path + '/' + file
        file = file.replace('//','/')
        df = pd.read_csv(file)
        # check the fields integrity
        if len(df.columns) < 12: # columns should be 'date','time','p','v','e','flag','b','bs','be','a','as','ae'
            # http://stackoverflow.com/questions/20297332/python-pandas-dataframe-retrieve-number-of-columns
            print('<--! {0} contains insufficient fields. --!>'.format(file))
            # write it to the log
            with open(_log_file, "a") as f:
                f.write("{0},missing field(s)".format(file))
            # inform it to admin via an email
            mail_title = ''
            mail_body = ''
            wbu.send_gmail(mail_title, mail_body)
        
# check the data content integrity
def check_es_data_integrity(_path, _target_exchange, _target_date, _log_file):

    for file in iou.getfiles(_path,'_0_{0}.csv'.format(_target_date)):

        file = _path + '/' + file
    
        # to check whether a character exists in a file : http://stackoverflow.com/questions/4940032/search-for-string-in-txt-file-python
        #import mmap
        with open(file, 'rb', 0) as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
            Mojibake_Characters = [b'\xfa', b'\xfb', b'\xfc', b'\xfd', b'\xfe']
            if (set(s) & set(Mojibake_Characters)) != set():    # see whether there is any 文字化け characters contained in the file : http://stackoverflow.com/questions/1388818/how-can-i-compare-two-lists-in-python-and-return-matches
                print('<--! {0} contains insufficient fields. --!>'.format(file))
                # write it to the log
                with open(_log_file, "a") as f:
                    f.write("{0},文字化け".format(file))
                # inform it to admin via an email
                mail_title = ''
                mail_body = ''
                wbu.send_gmail(mail_title, mail_body)

def treat_integrity_tickers(_target_date, _log_file):
    print('Recollecting the missing symbols ... ')
    failed_tickers = pd.read_csv(_log_file).ix[:,0]
    # recollecting the failed tickers
    file_of_recovery_list = 'G:/不足銘柄/{0}.csv'.format(_target_date)
    iou.write_csv(failed_tickers, file_of_recovery_list)
    # execute QCollector
    import subprocess
    win_shell = 'python "C:/Users/steve/Desktop/PySong2/eSignal/QCXE Data Interface.py" 不足銘柄 0'
    print(win_shell,'\n') # to check what been passed to win_shell
    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished   
    # inform it to admin via an email
    mail_title = 'Recollecting missing daily es ticks {0}'.format(_target_date)
    mail_body = 'missing tickers : {0}'.format(target_date)
    #wbu.send_gmail(mail_title, mail_body)

if __name__ == '__main__':
    
    #data_path = '../Data/raw/'
    #target_dates = ['2016-07-08','2016-07-09']
    #target_exchs = ['tse']#,'TSE']

    now = datetime.datetime.now()
    hour = now.hour
    minute = now.minute
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    two_days_ago = today + datetime.timedelta(days=-2)
    three_days_ago = today + datetime.timedelta(days=-3)
    four_days_ago = today + datetime.timedelta(days=-4)

    param = sys.argv
    if len(param) < 2:
        print('Wrong param given!')
        sys.exit()

    # assign parameter based on this process's current execution time
    # opening hours : https://en.wikipedia.org/wiki/List_of_stock_exchange_opening_times
    if hour==22 and minute >= 15 and minute <= 20:          # 9:00 JST to 11:30 JST, 12:30 JST to 15:00 JST (UTC+9)
        target_exchs = ['TSE']
    elif hour==22 and minute >= 30 and minute <= 35:        # 8:45 JST to 15:15 JST, 16:30 JST to  5:30 JST (Rubber : 16:30 JST to 19:00 JST)  (UTC+9)
        target_exchs = ['TOCOM']
    elif hour==23 and minute >= 10 and minute <= 15:        # 9:30 CST to 11:30 CST, 13:00 CST to 15:00 CST (UTC+8)
        target_exchs = ['SHG']  
    elif hour==23 and minute >= 30 and minute <= 35:        # 10:00 AEST to 16:00 AES (UTC+10)
        target_exchs = ['ASX']
    elif hour==2 and minute >= 10 and minute <= 15:        # 09:15 IST to 15:30 IST (UTC+5.5)
        target_exchs = ['Bombay']
    elif hour==8 and minute >= 45 and minute <= 50:         # 09:00 CET to 17:35 CET (UTC+1)
        target_exchs = ['Luxenberg']
    elif hour==10 and minute >= 0 and minute <= 5:           # PAN EUROPEAN EXCHANGE Where Retail Meets Institutions : 9:01 CET to 17:30 CET (UTC+1)
        target_exchs = ['EQUIDUCT']
    elif hour==10 and minute >= 25 and minute <= 30:         # 10:00	EET to 18:30 EET (UTC+2)
        target_exchs = ['OMX']  
    elif hour==11 and minute >= 15 and minute <= 20:         # 9:00 GMT to 16:00 GMT (UTC+0)
        target_exchs = ['BALTIC']
    elif hour==11 and minute >= 40 and minute <= 45:         # 08:00	GMT to 16:30 GMT (UTC+0)
        target_exchs = ['LME']
    elif hour==13 and minute >= 45 and minute <= 50:         # Various
        target_exchs = ['FUTURES']
    elif hour==14 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
        target_exchs = ['AMEX']
    elif hour==15 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
        target_exchs = ['NASDAQ']
    elif hour==16 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
        target_exchs = ['SP500']
    elif hour==18 and minute >= 10 and minute <= 15:        # 4:00 EST to 20:00 EST (UTC-5)
        target_exchs = ['ARCA']
    elif hour==20 and minute >= 30 and minute <= 35:        # Daily
        target_exchs = ['FUNDS', 'MarketStatistics']    
    else:
        exit()

    #target_exchs = [param[1]]  #'ASX'
    target_dates = [four_days_ago] #, three_days_ago, two_days_ago, yesterday, today]

    msg = 'Lunching 不足データ検出 ... '
    iou.console_title(msg + ' ' + str(now), 'shift-jis')

    for target_date in target_dates:    # loop through each date

        year = str(target_date).split('-')[0];
        month = str(target_date).split('-')[1].split('-')[0];
        day = str(target_date).split('-')[-1]
        if not dtu.is_weekday(year, month, day):
            continue

        log_file = './{0}_data_issue_log.csv'.format(target_date)
        for target_exch in target_exchs: # loop through each exchange

            ticker_file = 'G:/QCollector Expert For eSignal/tick_symbols_{0}.csv'.format(target_exch, target_date)
            if not iou.file_exist(ticker_file):
                continue
            tickers = pd.read_csv(ticker_file).ix[:,0]
            
            data_path = 'G:/QCollector Expert For eSignal/{0}/'.format(target_exch)

            # check to see whether missing files
            print('check eSignal data existance ... {0} {1}'.format(target_exch, target_date))
            check_es_daily_data_existance(data_path,target_exch,tickers,target_date,log_file)
            # check to see whether wrong historical data format
            print('check eSignal data field integrity ... {0} {1}'.format(target_exch, target_date))
            check_es_field_integrity(data_path,target_exch,target_date,log_file)
            # check to see whether 文字化け character contained in the downloaded data
            print('check eSignal data 文字化け ... {0} {1}'.format(target_exch, target_date))
            check_es_data_integrity(data_path,target_exch,target_date,log_file)

        # treat all the tickers with integrity issues
        treat_integrity_tickers(target_date, log_file)

    print('')