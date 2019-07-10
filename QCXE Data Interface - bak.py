# -*- coding:shift-jis -*-
import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PythonUtility/')

import IoUtility as iou
import OsUtility as osu
import DatetimeUtility as dtu
import DataTypeUtility as dpu
import HolidayUtility as hdu

import time
import datetime
from datetime import timedelta
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

def add_items(_wnd, _items, _portfolio, _interval, _days_back=1):
    # portfolio
    to_add_portfolio_combo = _wnd['ComboBox2']
    to_add_portfolio_combo.Select(_portfolio)
   
    #time.sleep(2)
    # to select 'Delete Items'
    #to_add_portfolio_combo.Click()
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
        add_items(_wnd, _items, _portfolio, _interval, _days_back)

def delete_items(_wnd, _items, _portfolio, _interval):
    # portfolio
    to_delete_portfolio_combo = _wnd['ComboBox2']
    to_delete_portfolio_combo.Select(_portfolio)
    # to select 'Add Items'
    #to_delete_portfolio_combo.Click()
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

def init_app():
    #アプリ起動
    app = Application()
    pass1 = 'D:/Program Files (x86)/QCollector Expert For eSignal/QCXEInterfaceClient.exe'
    test(app,pass1) 

    # focus the main window
    app_form = app['QCXE Data Interface Client']
    app_form.SetFocus()
    app_form.Click()

    # select [Client Data Requests] tab
    app_form['Edit0'].Click() # bottom [Messages] edit box
    app_form.TypeKeys('{TAB}{LEFT 2}')
    app_form['Get Portfolio List'].Click()
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

    now = datetime.datetime.now()
    hour = now.hour
    minute = now.minute
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    twodaysago = today + datetime.timedelta(days=-2)

    param = sys.argv    
    if len(param) == 3:
        portfolio = param[1]  #'ASX'
        interval = param[2]
    elif len(param) == 2:
        # assign parameter based on this process's current execution time
        # opening hours : https://en.wikipedia.org/wiki/List_of_stock_exchange_opening_times
        if hour==15 and minute >= 15 and minute <= 20:          # 9:00 JST to 11:30 JST, 12:30 JST to 15:00 JST (UTC+9)
            portfolio = 'TSE'
        elif hour==16 and minute >= 45 and minute <= 50:        # 8:45 JST to 15:15 JST, 16:30 JST to  5:30 JST (Rubber : 16:30 JST to 19:00 JST)  (UTC+9)
            portfolio = 'TOCOM'
        elif hour==17 and minute >= 45 and minute <= 50:        # 9:30 CST to 11:30 CST, 13:00 CST to 15:00 CST (UTC+8)
            portfolio = 'SHG'
        elif hour==20 and minute >= 0 and minute <= 5:        # 10:00 AEST to 16:00 AES (UTC+10)
            portfolio = 'ASX'
        elif hour==22 and minute >= 0 and minute <= 5:        # 09:15 IST to 15:30 IST (UTC+5.5)
            portfolio = 'Bombay'
        elif hour==1 and minute >= 45 and minute <= 50:         # 09:00 CET to 17:35 CET (UTC+1)
            portfolio = 'Luxenberg'
            exit() # way too many tickers
        elif hour==2 and minute >= 0 and minute <= 5:           # PAN EUROPEAN EXCHANGE Where Retail Meets Institutions : 9:01 CET to 17:30 CET (UTC+1)
            portfolio = 'EQUIDUCT'
            exit()
        elif hour==2 and minute >= 25 and minute <= 30:         # 10:00	EET to 18:30 EET (UTC+2)
            portfolio = 'OMX'
            exit() # way too many tickers
        elif hour==3 and minute >= 15 and minute <= 20:         # 9:00 GMT to 16:00 GMT (UTC+0)
            portfolio = 'BALTIC'
            exit()
        elif hour==3 and minute >= 40 and minute <= 45:         # 08:00	GMT to 16:30 GMT (UTC+0)
            portfolio = 'LME'
            exit()
        elif hour==5 and minute >= 45 and minute <= 50:         # Various
            portfolio = 'FUTURES'        
        elif hour==6 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
            portfolio = 'AMEX'
            exit()
        elif hour==7 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
            portfolio = 'NASDAQ'
            exit()
        elif hour==8 and minute >= 15 and minute <= 20:         # 09:30 EST to 16:00 EST (UTC-5)
            portfolio = 'SP500'
            exit()
        elif hour==10 and minute >= 10 and minute <= 15:        # 4:00 EST to 20:00 EST (UTC-5)
            portfolio = 'ARCA'
            exit()
        elif hour==12 and minute >= 30 and minute <= 35:        # Daily
            portfolio = 'FUNDS' #, 'MarketStatistics'
            exit()
        else:
            portfolio = 'TSE'
        interval = param[1]
    else:
        print('Wrong number of parameters has been given!')
        sys.exit()

    msg = 'Lunching QCollector Expert - with portfolio {0}'.format(portfolio)
    iou.console_title(msg + ' @ ' + str(now), 'shift-jis')

    # 前処理
    if portfolio!='不足銘柄':
        # 大容量ファイル(>1GB)は予め分割しておく
        import subprocess
        is_preprocessing = True
        win_shell = "python C:/Users/steve/Desktop/PySong2/eSignal/データ分割(DailyTick).py {0} {1}".format(portfolio, is_preprocessing)
        print(win_shell,'\n') # to check what been passed to win_shell
        cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
        #returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

        # rename xxx_0.csv file to xxx_0_yyyy-mm-dd.csv format
        #for file in iou.get_writable_files('G:/QCollector Expert For eSignal/{0}'.format(portfolio), '_0.csv'):
        #    file = 'G:/QCollector Expert For eSignal/' + portfolio + '/' + file
        #    try:
        #        date = pd.read_csv(file, header=None).iloc[0].ix[0]
        #        newfile = file.replace('.csv','_{0}.csv'.format(date.replace('/','-')))
        #        if iou.file_exist(newfile): # same file already exists
        #            #to_be_delete_file.append(file)
        #            iou.delete_file(x)   # delete
        #        else:
        #            iou.setReadOnly(file)
        #            os.rename(file, newfile)
        #    except:
        #        print('skipped {0}'.format(file))
    
    # QCollectorに追加する銘柄を選別
    if portfolio.lower()=='futures':

        # find previous business day
        for lookbackdays in [-1, -2, -3]:
            last_business_day = datetime.date.today() + timedelta(days=lookbackdays)
            if dtu.is_weekday(last_business_day.year, last_business_day.month, last_business_day.day):
                break
        # removing yesterday's active symbols for the exchange
        csv1 = pd.read_csv('G:/中心限月/eSignal/es_actives_list_{0}.csv'.format(last_business_day), header=None)    
        delete_tickers = '\r\n'.join(csv1.ix[:,0])  #１列目        
        # adding consolidated today's active symbols for the excahnge        
        last_day = last_business_day if hdu.is_a_holiday('Japan',today) else datetime.date.today() #'2016-05-28'
        try:
            csv2 = pd.read_csv('G:/中心限月/eSignal/es_actives_list_{0}.csv'.format(last_day), header=None)    
        except:
            csv2 = pd.read_csv('G:/中心限月/eSignal/es_actives_list_{0}.csv'.format(today), header=None)
        add_tickers = '\r\n'.join(csv2.ix[:,0])  #１列目

    elif portfolio.lower()=='tocom':

        # find previous business day in Japan
        for lookbackdays in [-1, -2, -3]:           
            last_business_day = datetime.date.today() + timedelta(days=lookbackdays)
            if not hdu.is_a_holiday('Japan',datetime.date(int(last_business_day.year),int(last_business_day.month),int(last_business_day.day))):
                break
        # removing yesterday's active symbols for the exchange        
        csv1 = pd.read_csv('G:/中心限月/tocom/{0}.csv'.format(last_business_day), header=None)    
        delete_tickers = '\r\n'.join(csv1.ix[:,0])  #１列目
        # adding consolidated today's active symbols for the excahnge
        today = last_business_day if hdu.is_a_holiday('Japan',today) else datetime.date.today() #'2016-05-28'
        csv2 = pd.read_csv('G:/中心限月/tocom/{0}.csv'.format(today), header=None)
        add_tickers = '\r\n'.join(csv2.ix[:,0])  #１列目

    elif portfolio=='不足銘柄':

        missing_ticker_list_file = osu.find_latest_file_in_dir('G:/不足銘柄/','.csv')
        missing_ticker_date = missing_ticker_list_file.replace('.csv', '')
        previous_business_date = dpu.str_to_date(missing_ticker_date) + datetime.timedelta(-1)
        while hdu.is_a_holiday('Japan', previous_business_date):
            previous_business_date = previous_business_date + datetime.timedelta(-1)

        # removing all eSignal registered symbols for the exchange
        try: # if the file exists
            csv1 = pd.read_csv('G:/不足銘柄/{0}.csv'.format(str(previous_business_date)), header=None)
            delete_tickers = '\r\n'.join(csv1.ix[:,0])  #１列目
        except:
            delete_tickers = []
        # adding consolidated yesterday's active symbols for the exchage
        csv2 = pd.read_csv('G:/不足銘柄/{0}'.format(missing_ticker_list_file), header=None)
        add_tickers = '\r\n'.join(x.split('_0_')[0] for x in csv2.ix[:,0])  #１列目

    else: # 現物株
        # removing all eSignal registered symbols for the exchange
        csv1 = pd.read_csv('G:/QCollector Expert For eSignal/Symbol {0}.csv'.format(portfolio), header=None)
        delete_tickers = '\r\n'.join(csv1.ix[:,0])  #１列目
        # adding consolidated yesterday's active symbols for the exchage
        csv2 = pd.read_csv('G:/QCollector Expert For eSignal/tick_symbols_{0}.csv'.format(portfolio), header=None)
        add_tickers = '\r\n'.join(csv2.ix[:,0])  #１列目
    #print(tickers)
    #print(TSE)

    while(True):
        try:
            app_form = init_app()
            break
        except:
            None

    print('QXCE was correctly launched!')
    #app_form.print_control_identifiers()
    if delete_tickers and (portfolio.lower()=='tocom' or portfolio.lower()=='futures'): # list not empty
        delete_items(app_form, [delete_tickers], portfolio, interval)
        waittime = max(min(len(csv1)/100, 300),10)   # minimum wait time 10sec, maximum wait time 300sec
        print('Entering Sleep {0} sec'.format(waittime))
        time.sleep(waittime)   # wait until deletion cmpletes
        app_form.Click()

    # lookback期間を決定
    if portfolio=='不足銘柄':
        day_diff = dtu.diff(missing_ticker_date, yesterday)
        days_back = max(0, day_diff)
    else:
        # if missing tick history data exists, download them all. 
        #largest_file = osu.find_largest_file_in_dir('G:/QCollector Expert For eSignal/{0}'.format(portfolio), '.csv')   # raw ticks
        #if largest_file=='':
        #    largest_file = osu.find_largest_file_in_dir('G:/QCollector Expert For eSignal/{0}/zipped'.format(portfolio), '.zip')   # zipped data 
        #ticker = largest_file.split('_0')[0]
        #csv_list = iou.getfiles('G:/QCollector Expert For eSignal/{0}'.format(portfolio), '.csv', ticker)
        #zip_list = iou.getfiles('G:/QCollector Expert For eSignal/{0}/zipped'.format(portfolio), '.zip', ticker)
        #print(len(csv_list), len(zip_list), ticker)
        ##last_updated_date = max([f.split('_0_')[1][:-4] for f in filelist]) if filelist else today
        #last_updated_date1 = max(csv_list).split('_0_')[1][:-4] if csv_list else dpu.date_to_str(today + datetime.timedelta(days=-90))
        #last_updated_date2 = max(zip_list).split('_0_')[1][:-4] if zip_list else dpu.date_to_str(today + datetime.timedelta(days=-90))
        #day_diff = dtu.diff(max(last_updated_date1,last_updated_date2), yesterday) + 1 # latest zip file might be short of data
        #if portfolio in ['NASDAQ','SP500','ARCA','EQUIDUCT']:
        #    days_back = max(1, day_diff)
        #else:
        #    days_back = max(0, day_diff)        
        dirs = osu.get_folder_names_in_a_folder('G:/QCollector Expert For eSignal/{0}/zipped/'.format(portfolio))
        if len(dirs) != 0:
            lastest_date = max(dirs)
            day_diff = dtu.diff(lastest_date, today) + 1 # latest zip file might be short of data
        else:
            day_diff = 90   # if no zipped folders, we just look back 90 days
        days_back = max(1, day_diff)
    add_items(app_form, [add_tickers], portfolio, interval, days_back)
    
    print('Successfully completed the operation.')

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
## Send this message to request a list of the QCollector portfolios. When QCollector recieves this message it will save a list of portofolio to a data file in the QCollector
## application data folder. Set LParam to the window handle where you want the portfolio list request complete message to be sent. wParam is not used.
#msgSend = win32api.RegisterWindowMessage('QCOLLECTOR_CLIENT_PORTFOLIO_LIST_REQUEST')
## After QCollector saves the portfolio list to a temp file, it will send this message to the hWnd that requested the portfolio list. lParam is an Atom holding one string
## value path and file name for the portfolio list file. QCollector 
#msgGet = win32api.RegisterWindowMessage('QCOLLECTOR_PORTFOLIO_LIST_REQUEST_COMPLETE')

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