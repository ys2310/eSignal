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
import DatetimeUtility as dtu
import DataTypeUtility as dpu
import HolidayUtility as hdu

import time
import datetime
from datetime import timedelta
from subprocess import call

import threading
import pandas as pd
import psutil

def Cppデータ加工(_exchange):
    import subprocess
    win_shell = "C:/Users/steve/Desktop/PySong2/x64/Release/CppBatch.exe {0}".format(_exchange)    
    #print(win_shell,'\n') # to check what been passed to win_shell
    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

def Transform_raw_eSignal_tick_into_level1_tick(_exchange):

    # @process details, @プロセス情報
    cmdline_search_str = 'データ加工_{0}.bat'.format(_exchange)
    found = osu.is_process_with_cmdline_exist(cmdline_search_str)

    # 既存プロセスなければ
    if not found:

        # an archive place to place zipped files
        dir_for_zipped_files = 'G:/QCollector Expert For eSignal/{0}/zipped/'.format(_exchange)
        osu.create_folder_if_not_exist(dir_for_zipped_files)

        Cppデータ加工(_exchange)

        #files = iou.getfiles('G:/QCollector Expert For eSignal/{0}/'.format(_exchange), '.csv')
        #dates = list(set([x.split('_0_')[1].replace('.csv','') for x in files]))
        #dates.sort()
        
        #m_thread = []
        #for date in dates:  # 日付優先で加工していく

        #    target_files = []
        #    [target_files.append(f) for f in files if date in f]

        #    t = threading.Thread(target=Cppデータ加工, args=[_exchange, target_files],name="thread {0}".format(date))
        #    m_thread.append(t)
        #    t.start()         

        #for worker in m_thread:
        #    print('joining {0}'.format(worker.getName()))
        #    worker.join()
    
        # move zipped files into a dir
        #cmd = 'cd /d G:/QCollector Expert For eSignal/{0}/ & move ./*.zip ./zipped/'.format(_exchange)
        #print(cmd)
        #returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished
        base_dir = 'G:/QCollector Expert For eSignal/{0}/'.format(_exchange)
        for f in iou.getfiles(base_dir, _ext='.zip'):
            osu.rename_file(base_dir + f, base_dir + 'zipped/' + f)

if __name__ == "__main__":

    today = datetime.date.today()
    now = datetime.datetime.now()
    hour = now.hour
    minute = now.minute

    param = sys.argv
    if len(param) < 2:  # exchange引数なし
        # assign parameter based on this process's current execution time
        # opening hours : https://en.wikipedia.org/wiki/List_of_stock_exchange_opening_times
        if hour==17 and minute >= 15 and minute <= 20:          # 9:00 JST to 11:30 JST, 12:30 JST to 15:00 JST (UTC+9)
            exchange = 'TSE'
        elif hour==17 and minute >= 30 and minute <= 35:        # 8:45 JST to 15:15 JST, 16:30 JST to  5:30 JST (Rubber : 16:30 JST to 19:00 JST)  (UTC+9)
            exchange = 'TOCOM'
        elif hour==18 and minute >= 10 and minute <= 15:        # 9:30 CST to 11:30 CST, 13:00 CST to 15:00 CST (UTC+8)
            exchange = 'SHG'
        elif hour==18 and minute >= 30 and minute <= 35:        # 10:00 AEST to 16:00 AES (UTC+10)
            exchange = 'ASX'
        elif hour==21 and minute >= 10 and minute <= 15:        # 09:15 IST to 15:30 IST (UTC+5.5)
            exchange = 'Bombay'
        elif hour==3 and minute >= 45 and minute <= 50:         # 09:00 CET to 17:35 CET (UTC+1)
            exchange = 'Luxenberg'
        elif hour==5 and minute >= 0 and minute <= 5:           # PAN EUROPEAN EXCHANGE Where Retail Meets Institutions : 9:01 CET to 17:30 CET (UTC+1)
            exchange = 'EQUIDUCT'
        elif hour==5 and minute >= 25 and minute <= 30:         # 10:00	EET to 18:30 EET (UTC+2)
            exchange = 'OMX'
        elif hour==6 and minute >= 15 and minute <= 20:         # 9:00 GMT to 16:00 GMT (UTC+0)
            exchange = 'BALTIC'
        elif hour==6 and minute >= 40 and minute <= 45:         # 08:00	GMT to 16:30 GMT (UTC+0)
            exchange = 'LME'
        elif hour==8 and minute >= 45 and minute <= 50:         # Various
            exchange = 'FUTURES'
        elif hour==9 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
            exchange = 'AMEX'
        elif hour==10 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
            exchange = 'NASDAQ'
        elif hour==11 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
            exchange = 'SP500'
        elif hour==13 and minute >= 10 and minute <= 15:        # 4:00 EST to 20:00 EST (UTC-5)
            exchange = 'ARCA'
        elif hour==15 and minute >= 30 and minute <= 35:        # Daily
            exchange = 'FUNDS' #, 'MarketStatistics'
        else:
            exit()
    else:
        exchange = param[1]  #'ASX'

    msg = 'Lunching データ加工 - with portfolio {0}'.format(exchange)
    iou.console_title(msg + ' @ ' + str(now), 'shift-jis')

    Transform_raw_eSignal_tick_into_level1_tick(exchange)
