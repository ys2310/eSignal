# -*- coding:shift-jis -*-
import getpass
user = getpass.getuser()

import os
try:
    currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
    os.chdir(currentWorkingDirectory)
except:
    currentWorkingDirectory = "C:\\Users\\sou_u\\Desktop\\PySong2\\eSignal".format(user)
    os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')

import IoUtility as iou
import OsUtility as osu
import DatetimeUtility as dtu
import DataTypeUtility as dpu
import HolidayUtility as hdu
import WebUtility as wbu

import time
import datetime
from datetime import timedelta
import subprocess
from subprocess import call

import pandas as pd

import re
import pyautogui

import pywinauto
from pywinauto.application import Application

def test(app1,pass1):
    try:
        app1.Connect(path = pass1)
    except:
        app1.Start(cmd_line = pass1)
        time.sleep(2)

def add_items(_wnd, _items, _exchange, _interval, _days_back=1):
    # exchange
    to_add_exchange_combo = _wnd['ComboBox2']
   
    if "ChiX" in _exchange or "SBI_Japan" in _exchange or "SouthEastAsia" in _exchange or "SHZ" in _exchange or "SHG" in _exchange or "HKG" in _exchange  or "TSE" in _exchange  or "ASX" in _exchange:
       to_add_exchange_combo.Select("hot_asia")
    elif "AMEX" in _exchange or "ARCA" in _exchange or "NASDAQ" in _exchange or "NYSE" in _exchange or "SP500" in _exchange or "USOTC" in _exchange:
       to_add_exchange_combo.Select("hot_asia")
    else:
       to_add_exchange_combo.Select("hot_europe")
#    to_add_exchange_combo.Select(_exchange) # before 2018.03.04
    
    #time.sleep(2)
    # to select 'Delete Items'
    #to_add_exchange_combo.Click()
    #_wnd.SetFocus()
    #_wnd.TypeKeys("{TAB}")
    #_wnd.TypeKeys("{LEFT}")
    try:
        app_form['Edit0'].Click() # bottom [Messages] edit box
        app_form.TypeKeys('{TAB 3}{LEFT}')    
        # Tick, 1min or Daily
        bar_interval_combo = _wnd['ComboBox1']
        bar_interval_combo.Select(_interval) # 0:tick, 1:1min, Daily:daily
        # days back
        days_back_textbox = _wnd['Edit3']
        days_back_textbox.SetText(_days_back)
        # Symbol list
        to_add_items_textbox = _wnd['Edit2']
        to_add_items_textbox.SetText('\r\n'.join(_items))
        # update histrical data
        update_item_checkbox = _wnd['Update each item with history dataCheckBox']
        if update_item_checkbox.GetCheckState() == 0:
                update_item_checkbox.Click()
        # delete button
        add_btn = _wnd['Add/Update Items NowButton']
        add_btn.Click()
    except:
        print('Retring add_items...')
        add_items(_wnd, _items, _exchange, _interval, _days_back)

def delete_items(_wnd, _items, _exchange, _interval):
    # exchange
    to_delete_exchange_combo = _wnd['ComboBox2']

    if "ChiX" in _exchange or "SBI_Japan" in _exchange or "SouthEastAsia" in _exchange or "SHZ" in _exchange or "SHG" in _exchange or "HKG" in _exchange  or "TSE" in _exchange  or "ASX" in _exchange:
       to_delete_exchange_combo.Select("hot_asia")
    elif "AMEX" in _exchange or "ARCA" in _exchange or "NASDAQ" in _exchange or "NYSE" in _exchange or "SP500" in _exchange or "USOTC" in _exchange:
       to_delete_exchange_combo.Select("hot_asia")
    else:
       to_delete_exchange_combo.Select("hot_europe")
#    to_delete_exchange_combo.Select(_exchange) # before 2018.03.04

    # to select 'Add Items'
    #to_delete_exchange_combo.Click()
    #_wnd.SetFocus()
    #_wnd.TypeKeys("{TAB}")
    #_wnd.TypeKeys("{RIGHT}")
    app_form['Edit0'].Click() # bottom [Messages] edit box
    app_form.TypeKeys('{TAB 3}{RIGHT}')    
    # Tick, 1min or Daily
    bar_interval_combo = _wnd['ComboBox3']
    bar_interval_combo.Select(_interval)     # 0:tick, 1:1min, Daily:daily
    # Symbol list
    to_delete_items_textbox = _wnd['Edit2']
    to_delete_items_textbox.SetText('\r\n'.join(_items))
    # delete button
    delete_btn = _wnd['Delete Items NowButton']
    delete_btn.Click()

def init_app(_path_to_exe):
    #アプリ起動
    app = Application()
    pass1 = _path_to_exe
    test(app,pass1) 

    # focus the main window
    app_form = app['QCXE Data Interface Client']
    app_form.SetFocus()
    app_form.Click()

    # select [Client Data Requests] tab
    app_form['Edit0'].Click() # bottom [Messages] edit box
    app_form.TypeKeys('{TAB}{LEFT 2}')
    app_form['Get portfolio List'].Click()
    app_form['Edit0'].Click() # bottom [Messages] edit box
    app_form.TypeKeys('{TAB}{RIGHT}')
    #app_form.TypeKeys('{RIGHT 2}')
    return app_form

# TSE : 9:00～15:15 - 2h45m
# SHG : - 4h
# ASX : - 4h30m ～ 5h30m
# HKG : - 5h30m
# DME (一部 Daily) :  - 35m
# BALTIC (1min) : 19:00 or 翌1:00 - 20m
# FUNDS (Daily) : 
# NASDAQ : 17:00～翌5:00 - 15m
# S&P500 : 
# ARCA : 17:00～翌9:00 - 
# LME : 
# ARCA : - 1h45m
# EQUIDUCT : 16:00～翌0:35 - 3h30m
# ActiveFutures : 
# ETF : 9:00～翌2:30 - 2h30m 
# SouthEastAsiaStock : 10h40m
# AsiaFutures : 
#
# Chi-X Japan : 
# SBI Japanext : 
# Phillipine : 10:30～16:20 - 
# U.S.OTC : 22:30 - 翌5:00
# 
if __name__ == "__main__":

    print('Inside of QCXE Data Interface')    

    start_now = datetime.datetime.now()
    hour = start_now.hour
    minute = start_now.minute
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    twodaysago = today + datetime.timedelta(days=-2)    

    param = sys.argv

    # main function starts    
    print("引数の総個数 = {0}".format(len(param)))
    #if len(sys.argv) != 2:
    #    print("引数を正しく指定してください！")
    for i,x in enumerate(param):
        print("{0}番目の引数 = {1}\n".format(i, x))    
    time.sleep(5)

    if len(param) == 2: # 引数1つ
        # assign parameter based on this process's current execution time
        # opening hours : https://en.wikipedia.org/wiki/List_of_stock_exchange_opening_times
        exchange_time_mapping_dict = { 1515:'TSE',     1645:'TOCOM', 1745:'SHG', 1900:'HKG',  2030:'ASX',   2200:'Bombay',  145:'Luxenberg', 315:'BALTIC', 200:'EQUIDUCT', 225:'OMX', 340:'LME', # evening part
                                        545:'FUTURES',  615:'AMEX',   715:'NASDAQ', 817:'SP500', 1010:'ARCA',   1230:'FUNDS', 2355:'SHZ', # morning part
                                      }
        # TSE       : 9:00 JST to 11:30 JST, 12:30 JST to 15:00 JST (UTC+9)
        # TOCOM     : 8:45 JST to 15:15 JST, 16:30 JST to  5:30 JST (Rubber : 16:30 JST to 19:00 JST)  (UTC+9)
        # SHG       : 9:30 CST to 11:30 CST, 13:00 CST to 15:00 CST (UTC+8)
        # HKG       : 9:30 CST to 16:00 CST (UTC+8)
        # ASX       ; 09:15 IST to 15:30 IST (UTC+5.5)
        # Bombay    : 09:00 CET to 17:35 CET (UTC+1)
        # Luxenberg : 09:00 - 17:35 CET(UTC+1)        <- task scheduler トリガー削除済  ToDo : Not Entitled 
        # EQUIDUCT  : PAN EUROPEAN EXCHANGE Where Retail Meets Institutions : 9:01 CET to 17:30 CET (UTC+1)
        # OMX       : 10:00	EET to 18:30 EET (UTC+2)
        # BALTIC    : 9:00 GMT to 16:00 GMT (UTC+0)   <- task scheduler トリガー削除済  ToDo : データ整備対応
        # LME       : 08:00	GMT to 16:30 GMT (UTC+0)
        # FUTURES   : Various
        # AMEX      : 09:30 EST to 16:00 EST (UTC-5)
        # NASDAQ    : 09:30 EST to 16:00 EST (UTC-5)
        # SP500     : 09:30 EST to 16:00 EST (UTC-5)
        # ARCA      : 4:00 EST to 20:00 EST (UTC-5)
        # FUNDS     : Daily                           <- task scheduler トリガー削除済  ToDo : データ整備対応

        #print(hour * 100 + minute)
        exhcnage_whose_raw_tick_data_to_be_processed = None
        time_as_int = hour * 100 + minute
        for time_range in range(time_as_int, time_as_int - 6, -1): # in a 5 minute time interval
            try:
                exhcnage_whose_raw_tick_data_to_be_processed = exchange_time_mapping_dict[time_range] # yield error when no matched key
            except:
                None

        exchange = 'ASX' if (exhcnage_whose_raw_tick_data_to_be_processed is None) else exhcnage_whose_raw_tick_data_to_be_processed 
        interval = param[1] # tick, 1min, 5min, hourly, daily

    elif len(param) == 3: # 引数2つ

        exchange = param[1]  #'ASX'
        interval = param[2]
    else:
        print('Wrong number of parameters has been given!')
        sys.exit()

    msg = 'Launching QCollector Expert - exchange:{0} interval:{1}'.format(exchange,interval)
    print(msg)
    iou.console_title(msg + ' @ ' + str(start_now), 'shift-jis')

    # 前処理    
    is_preprocessing = True
#    win_shell = "python C:/Users/steve/Desktop/PySong2/eSignal/データ分割(DailyTick).py {0} {1}".format(exchange, is_preprocessing)
#    print(win_shell,'\n') # to check what been passed to win_shell
#    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    #returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished
    folder = 'G:/QCollector_Expert_For_eSignal/{0}/'.format(exchange)
    # ======== rename raw tick files _0.csv  ========
    print('======== renaming raw tick files _0.csv  ========')
    # on linux this works : 
    # >sudo chown p000505:p000505 QCollector_Expert_For_eSignal/TSE/*.csv
    # >stat QCollector_Expert_For_eSignal/TSE/*-TSE_0.csv | sed -r 's/[:+.]+//g' | sed -r 's/[ ]/_/g' | egrep Modify.* | sed -r 's/Modify_(.{17}).*/\1 /' | (read string; rename "s/TSE_0.csv/TSE_\@$string\@_0.csv/g"  QCollector_Expert_For_eSignal/TSE/*-TSE_0.csv)

#    linux_cmd = '"C:/Program Files/plink_0.67/plink.exe" -pw M9rgan?? p000505@192.168.0.9 "/home/p000505/QCollector_Expert_For_eSignal/rename_eSignal_raw.sh {0}" '.format(exchange)
    linux_cmd = '"G:/QCollector_Expert_For_eSignal/rename_eSignal_raw.sh {0}" '.format(exchange)
    result,err = subprocess.Popen(linux_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()

    """ obsolete : this took long time for low CPU and files over the network 
    for csv_file in iou.getfiles(folder,'_0.csv'):
        csv_file = (folder + '/' + csv_file).replace('//','/').replace('\\','/')
        last_update = osu.get_file_lastupdate(csv_file)
        #if 'ASX2017-07-30' in csv_file:
            #osu.rename_file(csv_file, csv_file.replace('ASX2017-07-30','ASX'))
        if not csv_file.count('@') >= 2: # exclude files already has datetime in its name. ex.@2017-07-30 172320@
            print('renaming ',csv_file)
            osu.rename_file(csv_file, csv_file.replace('_0.csv','_@'+dpu.datetime_to_str(last_update).replace(':','')[:-7]+'@_0.csv'))
            """

    # QCollectorに追加する銘柄を選別
    delete_tickers = ''
    add_tickers = ''
    if exchange.lower()=='futures':

        # find previous business day
        for lookbackdays in [-1, -2, -3]:
            last_business_day = datetime.date.today() + timedelta(days=lookbackdays)
            if dtu.is_weekday(last_business_day.year, last_business_day.month, last_business_day.day):
                break
        # removing yesterday's active symbols for the exchange
        last_ticker_file = 'G:/中心限月/eSignal/es_actives_list_{0}.csv'.format(last_business_day)
        if iou.file_exist(last_ticker_file):
            csv1 = pd.read_csv(last_ticker_file, header=None, engine='python')
            delete_tickers += '\r\n'.join(csv1.ix[:,0])  #１列目
        # adding consolidated today's active symbols for the excahnge
        last_day = last_business_day if hdu.is_a_holiday('Japan',today) else datetime.date.today() #'2016-05-28'
        previous_business_day_ticker_file = 'G:/中心限月/eSignal/es_actives_list_{0}.csv'.format(last_day)
        today_ticker_file = 'G:/中心限月/eSignal/es_actives_list_{0}.csv'.format(today)
        if iou.file_exist(previous_business_day_ticker_file):
            csv2 = pd.read_csv(previous_business_day_ticker_file, header=None, engine='python') # engine='python' option for "OSError: Initializing from file failed" run-time error 日本語マルチバイト文字のため http://own-search-and-study.xyz/2017/04/08/python3-6%E3%81%AEpandas%E3%81%A7%E3%80%8Cinitializing-from-file-failed%E3%80%8D%E3%81%8C%E8%B5%B7%E3%81%8D%E3%81%9F%E5%A0%B4%E5%90%88%E3%81%AE%E5%AF%BE%E7%AD%96/
            #add_tickers = [x+'\r\n' for x in csv2[0] if x != '?']
            add_tickers += '\r\n'.join(csv2.ix[:,0])  #１列目
        elif iou.file_exist(today_ticker_file):
            csv2 = pd.read_csv(today_ticker_file, header=None, engine='python')
            #add_tickers += '\r\n'.join(csv2.ix[:,0].astype(str))  #１列目
            for x in csv2.ix[:,0]:
                if x==None:
                    print(x)
            #add_tickers = sum([x+'\r\n' for x in csv2[0] if x != '?'],[])
            add_tickers += '\r\n'.join(x for x in csv2[0] if x != '?')  #１列目
            #print(add_tickers)

    elif exchange.lower()=='tocom':

        # find previous business day in Japan
        for lookbackdays in [-1, -2, -3]:
            last_business_day = datetime.date.today() + timedelta(days=lookbackdays)
            if not hdu.is_a_holiday('Japan',datetime.date(int(last_business_day.year),int(last_business_day.month),int(last_business_day.day))):
                break
        # removing yesterday's active symbols for the exchange
        csv1 = pd.read_csv('G:/中心限月/tocom/{0}.csv'.format(last_business_day), header=None, engine='python')
        delete_tickers = '\r\n'.join(csv1.ix[:,0])  #１列目
        # adding consolidated today's active symbols for the excahnge
        today = last_business_day if hdu.is_a_holiday('Japan',today) else datetime.date.today() #'2016-05-28'
        csv2 = pd.read_csv('G:/中心限月/tocom/{0}.csv'.format(today), header=None, engine='python')
        add_tickers = '\r\n'.join(csv2.ix[:,0])  #１列目
        #add_tickers = [x+'\r\n' for x in csv2[0] if x != '?']

    else: # 現物株
        # removing all eSignal registered symbols for the exchange
        csv1 = pd.read_csv('C:/QCollector_Expert_For_eSignal/tick_symbols_{0}.csv'.format(exchange), header=None, engine='python')
        delete_tickers = '\r\n'.join(csv1.ix[:,0])  #１列目
        # adding consolidated yesterday's active symbols for the exchage
        csv2 = pd.read_csv('C:/QCollector_Expert_For_eSignal/tick_symbols_{0}.csv'.format(exchange), header=None, engine='python')
        add_tickers = '\r\n'.join(csv2.ix[:,0])  #１列目
        #add_tickers = [x+'\r\n' for x in csv2[0] if x != '?']
    #print(tickers)
    #print(TSE)

    while(True):
        try:
            path_to_exe = 'C:/Program Files (x86)/QCollector Expert For eSignal/QCXEInterfaceClient.exe'
            app_form = init_app(path_to_exe)
            break
        except:
            None

    print('QXCE was correctly launched!')
    #app_form.print_control_identifiers()
    if delete_tickers: # and (exchange.lower()=='tocom' or exchange.lower()=='futures'): # list not empty
        delete_items(app_form, [delete_tickers], exchange, interval)
        waittime = max(min(len(csv1)/100, 300),10)   # minimum wait time 10sec, maximum wait time 300sec
        print('Entering Sleep {0} sec'.format(waittime))
        time.sleep(waittime)   # wait until deletion cmpletes
        app_form.Click()

    # lookback期間を決定   
    # if missing tick history data exists, download them all. 
    #largest_file = osu.find_largest_file_in_dir('G:/QCollector_Expert_For_eSignal/{0}'.format(exchange), '.csv')   # raw ticks
    #if largest_file=='':
    #    largest_file = osu.find_largest_file_in_dir('G:/QCollector_Expert_For_eSignal/{0}/zipped'.format(exchange), '.zip')   # zipped data 
    #ticker = largest_file.split('_0')[0]
    #csv_list = iou.getfiles('G:/QCollector_Expert_For_eSignal/{0}'.format(exchange), '.csv', ticker)
    #zip_list = iou.getfiles('G:/QCollector_Expert_For_eSignal/{0}/zipped'.format(exchange), '.zip', ticker)
    #print(len(csv_list), len(zip_list), ticker)
    ##last_updated_date = max([f.split('_0_')[1][:-4] for f in filelist]) if filelist else today
    #last_updated_date1 = max(csv_list).split('_0_')[1][:-4] if csv_list else dpu.date_to_str(today + datetime.timedelta(days=-90))
    #last_updated_date2 = max(zip_list).split('_0_')[1][:-4] if zip_list else dpu.date_to_str(today + datetime.timedelta(days=-90))
    #day_diff = dtu.diff(max(last_updated_date1,last_updated_date2), yesterday) + 1 # latest zip file might be short of data
    #if exchange in ['NASDAQ','SP500','ARCA','EQUIDUCT']:
    #    days_back = max(1, day_diff)
    #else:
    #    days_back = max(0, day_diff)        
    dirs = osu.get_folder_names_in_a_folder('G:/QCollector_Expert_For_eSignal/{0}/zipped/'.format(exchange))
    if dirs and len(dirs) != 0:
        lastest_date = max(dirs)
        day_diff = dtu.diff(lastest_date, today) + 1 # latest zip file might be short of data
    else:
        day_diff = 90   # if no zipped folders, we just look back 90 days
    days_back = max(1, day_diff)
    if add_tickers:
        add_items(app_form, [add_tickers], exchange, interval, days_back)
    else:
        exit
    
    body_text = 'Operation started at {0}'.format(start_now)
    wbu.send_gmail('QCXE {0} 完了通知【{1}】'.format(exchange, dtu.get_currnet_datetime_str()), body_text)
    print('Leaving ... QCXE Data Interface.py main thread ... !')

    import win32com.client
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys("{ENTER}", 0) 
    
## Move C:\Python34\Lib\site-packages\pywin32_system32\pythoncom34.dll,pywintypes34.dll
## to
## C:\Python34\Lib\site-packages\win32
## ref : http://yuan-jiu.asablo.jp/blog/2014/04/08/7270185
## ref : http://www.programcreek.com/python/example/13936/win32api.RegisterWindowMessage
## ref : http://stackoverflow.com/questions/14788036/python-win32api-sendmesage
## ref : http://d.hatena.ne.jp/fgshun/20090913/1252813676
#import win32api, win32gui, win32com, win32con
#import os
#import tempfile
## register msg
## Send this message to request a list of the QCollector exchanges. When QCollector recieves this message it will save a list of portofolio to a data file in the QCollector
## application data folder. Set LParam to the window handle where you want the exchange list request complete message to be sent. wParam is not used.
#msgSend = win32api.RegisterWindowMessage('QCOLLECTOR_CLIENT_exchange_LIST_REQUEST')
## After QCollector saves the exchange list to a temp file, it will send this message to the hWnd that requested the exchange list. lParam is an Atom holding one string
## value path and file name for the exchange list file. QCollector 
#msgGet = win32api.RegisterWindowMessage('QCOLLECTOR_exchange_LIST_REQUEST_COMPLETE')

#hWnd = win32gui.FindWindow('QCDataInterfaceWndClass', None)
##hRecv, tpath = tempfile.mkstemp() #os.fdopen("./file.txt", "w") #This.Handle.ToInt32
#hRecv = os.open('./temp.txt', os.O_RDWR|os.O_CREAT)
##hRecv = os.fdopen(fd, "w") # win32gui.GetForegroundWindow() #os.fdopen('./temp.txt', "w")
#print(hRecv)

#if hWnd != 0:
#    res = win32gui.SendMessage(hWnd, msgSend, 0, hRecv)
#   # hRecv.close()
#    print(hRecv) if res!=0 else None
#    recv = win32gui.GetMessage(None, 0, 0)
#    print(recv)