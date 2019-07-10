# -*- coding:shift-jis -*-

import os
#os.chdir('')

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')
sys.path.append('../../PythonUtility/')

import IoUtility as iou
import VisualUtility as vsu

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

def get_today_histrical_data(_portfolio):
    
    #アプリ起動
    app = Application() 
    pass1 = 'D:/Program Files (x86)/QCollector Expert For eSignal/QCollectorXE.exe'
    test(app,pass1)

    app_form = app['QCollector Expert']
    try:
        app_form.SetFocus()
    except:
        print(' ==== Recalling function ==== ')
        get_today_histrical_data(_portfolio)

    _wnd = app_form

    _wnd.SetFocus()
    EventWnd = _wnd['TPBRichEdit20']
    EventWnd.Click()
    _wnd.SetFocus()
    _wnd.TypeKeys("{TAB}")
    #EventWnd = _wnd['TVirtualStringTree']
    #EventWnd.Click()
    #tmp = _wnd['TVirtualStringTree0']
    #tmp.SetForcus()
    _wnd.SetFocus()
    _wnd.TypeKeys("{HOME}")
    if _portfolio=='ARCA':
        print('downloading ARCA')
    elif _portfolio=='AsiaFutures':
        print('downloading AsiaFutures')
        _wnd.TypeKeys("{DOWN 1}")
    elif _portfolio=='ASX':                     # 4h, 30m ～ 5h, 30m
        print('downloading ASX')
        _wnd.TypeKeys("{DOWN 2}")
    elif _portfolio=='BALTIC':
        print('downloading BALTIC')
        _wnd.TypeKeys("{DOWN 3}")
    elif _portfolio=='CME':
        print('downloading CME')
        _wnd.TypeKeys("{DOWN 4}")
    elif _portfolio=='DME':
        print('downloading DME')
        _wnd.TypeKeys("{DOWN 5}")
    elif _portfolio=='EQUIDUCT':
        print('downloading EQUIDUCT')
        _wnd.TypeKeys("{DOWN 6}")
    elif _portfolio=='ETF':                     # 2h, 30m
        print('downloading ETF')
        _wnd.TypeKeys("{DOWN 7}")
    elif _portfolio=='FUNDS':
        print('downloading FUNDS')
        _wnd.TypeKeys("{DOWN 8}")
    elif _portfolio=='HKG':                     # 5h, 30m
        print('downloading HKG')
        _wnd.TypeKeys("{DOWN 9}")
    elif _portfolio=='LME':
        print('downloading LME')
        _wnd.TypeKeys("{DOWN 10}")
    elif _portfolio=='MarketStatistics':
        print('downloading MarketStatistics')
        _wnd.TypeKeys("{DOWN 11}")
    elif _portfolio=='NASDAQ':
        print('downloading NASDAQ')
        _wnd.TypeKeys("{DOWN 12}")
    elif _portfolio=='S&P500':
        print('downloading S&P500')
        _wnd.TypeKeys("{DOWN 13}")
    elif _portfolio=='SHG':
        print('downloading SHG')                # 4h
        _wnd.TypeKeys("{DOWN 14}")
    elif _portfolio=='SouthEastAsiaStock':
        print('downloading SouthEastAsiaStock') # 10h,40m
        _wnd.TypeKeys("{DOWN 15}")
    elif _portfolio=='TSE':                     # 2h, 45m
        print('downloading TSE')
        _wnd.SetFocus()
        _wnd.TypeKeys("{DOWN 16}")
    # select adjust start date
    _wnd.SetFocus()
    _wnd.TypeKeys("%{P}")
    _wnd.SetFocus()
    _wnd.TypeKeys("{DOWN 9}")
    _wnd.SetFocus()
    _wnd.TypeKeys("{LEFT}")    
    _wnd.SetFocus()
    _wnd.TypeKeys("{ENTER}")
    sub_form = app['Adjust First Date For Multiple Items']
    try:
        sub_form.SetFocus()    
        #sub_form.print_control_identifiers()
        #DatePicker = sub_form['TDateTimePicker']
        #DatePicker.SetText('2016/08/08')
        OkButton = sub_form['OKTBitBtn']
        OkButton.Click()
        confirm_form = app['Confirm']
        confirm_form['OK'].Click()
    except:
        print(' ==== Recalling function ==== ')
        get_today_histrical_data(_portfolio)

def import_items(_wnd, _items, _portfolio):
    d

if __name__ == "__main__":

    param = sys.argv
    if len(param) < 2:
        print('No param given!')
        sys.exit()

    portfolio = param[1] #'TSE'
    print('Lunching QCollector Expert - with portfolio {0}'.format(portfolio))
    #TSE = pd._csv('F:/QCollector Expert For eSignal/Symbol TSE.csv')
    #TSE = '\r\n'.join(TSE.ix[:,0])
    #print(TSE)
    
    for file in iou.get_writable_files('F:/QCollector Expert For eSignal/{0}'.format(portfolio), '_0.csv'):
        file = 'G:/QCollector Expert For eSignal/' + portfolio + '/' + file
        try:
            date = pd.read_csv(file, header=None).iloc[0].ix[0]
            iou.setReadOnly(file)
            os.rename(file, file.replace('.csv','_{0}.csv'.format(date)))    
        except:
            print('skipped {0}'.format(file))
    
    get_today_histrical_data(portfolio)

    app_form.SetFocus()
    #app_form.TypeKeys("{TAB}")

    #app_form.print_control_identifiers()
    #items = [] #]
    ##delete_items(app_form, items, portfolio)
    #add_items(app_form, items, portfolio, 0)
    
    print('')

