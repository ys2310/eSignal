# -*- coding:shift-jis -*-
import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PythonStats/x64/Release/')
sys.path.append('C:/Users/steve/Desktop/PySong2/x64/Release/') # for RollOver.pyd
sys.path.append('../PyUtility/')

import OsUtility as osu
import IoUtility as iou
import WebUtility as wbu
import DataTypeUtility as dpu
import DatetimeUtility as dtu
#import DbUtility as dbu

import time
import datetime
import threading
from subprocess import call

import html5lib
from bs4 import  BeautifulSoup 
import urllib.request as ur
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#total_num_of_open_browser = 0

def GetTocomActiveMonth(_save_file, _year, _month):
    try:
        import CppRollOver
        first = CppRollOver.calc_rollover(_year, _month)
        second = CppRollOver.calc_rollover(_year, _month+1)
        ric_list = list(set(first + second))    
        col_header = [1]
        col_num = len(col_header)
        row_num = int(len(ric_list)/len(col_header))
        isHeader = False
        #ric_list.sort()
        iou.write_csv(ric_list, _save_file, False, row_num, col_num, isHeader, col_header)
        print('\n\n')
    except:
        pass

def list_of_list_to_csv(_contracts):
    import csv
    with open("output.csv", "wb") as f:
        writer = csv.writer(f)
        for contract in _contracts:            
            writer.writerows(contract)

def list_of_list_to_excel(filename, sheet, list1, list2, x, y, z):
    import xlwt
    book = xlwt.Workbook()
    sh = book.add_sheet(sheet)

    variables = [x, y, z]
    x_desc = 'Display'
    y_desc = 'Dominance'
    z_desc = 'Test'
    desc = [x_desc, y_desc, z_desc]

    col1_name = 'Stimulus Time'
    col2_name = 'Reaction Time'

    #You may need to group the variables together
    #for n, (v_desc, v) in enumerate(zip(desc, variables)):
    for n, v_desc, v in enumerate(zip(desc, variables)):
        sh.write(n, 0, v_desc)
        sh.write(n, 1, v)

    n+=1

    sh.write(n, 0, col1_name)
    sh.write(n, 1, col2_name)

    for m, e1 in enumerate(list1, n+1):
        sh.write(m, 0, e1)

    for m, e2 in enumerate(list2, n+1):
        sh.write(m, 1, e2)

    book.save(filename)

def dataframe_to_excel(_dataframe, _excel_name, _sheet_name):
  
    #if os.path.exists(_excel_name):             # append to existing file
        
    #    from openpyxl import load_workbook
    #    book = load_workbook(_excel_name)

    #    print('Appending excel sheet {0}'.format(_sheet_name))
    #    # http://stackoverflow.com/questions/29974672/writing-pandas-dataframe-to-excel-with-different-formats-for-different-columns
    #    writer = pd.ExcelWriter(_excel_name, engine='openpyxl')
    #    writer.book = book
    #    _dataframe.to_excel(writer, sheet_name=_sheet_name, index=False)
    #    writer.save()

    #else:                                       # create a new file
    if "?" in _excel_name or "?" in _sheet_name:
        return
    print('Generating excel file {0}'.format(_excel_name))
    _dataframe.to_excel(_excel_name, sheet_name=_sheet_name, index=False)
    #_dataframe.to_csv(_excel_name, index=False)

#g_contract_data_frame = []
#g_outfile = []
#g_exch_smb = []
def get_contracts_list_by_exch_from_barchart(_outfile, _download_url, _exch_smb, _retry_times=0):

    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options

    #url = 'http://www.barchart.com/futures/price-by-exchange/' + _exch_smb
    
    url = _download_url  #'https://www.barchart.com/futures/prices-by-exchange/' + _exch_smb
    print(url)

	# use SELENIUM to select next menu (iBoxx index)
    print('Openning webdriver ... {0}'.format(url))
    #chrome_options = Options()  
    #chrome_options.add_argument("--headless") # https://duo.com/blog/driving-headless-chrome-with-python https://stackoverflow.com/questions/16180428/can-selenium-webdriver-open-browser-windows-silently-in-background
    #chrome_options.binary_location = "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe"
    # need to open driver connection every time other it's just too fast that correct page won't load correctly.
    driver = webdriver.Chrome(executable_path='./WebDriver/chromedriver.exe') #, chrome_options=chrome_options) # webdriver.PhantomJS('./phantomjs-2.1.1-windows/bin/phantomjs.exe') # http://zipsan.hatenablog.jp/entry/20150413/1428861548 # webdriver.Chrome('./chromedriver.exe') #    
    #driver.set_window_position(500, 100) # minimize the window https://stackoverflow.com/questions/42476994/how-to-hide-chrome-driver-in-python https://stackoverflow.com/questions/42644500/selenium-python-minimize-browser-window
    #driver.set_window_size(250, 250)
    print('Getting url ... {0}'.format(_exch_smb))
    try: 
        driver.get(url)
    except: # timeout error could happen
        driver.close()
        _retry_times = _retry_times + 1
        if _retry_times > 3:
            return
        get_contracts_list_by_exch_from_barchart(_outfile, _download_url, _exch_smb, _retry_times)

    # wait till the page loads : http://stackoverflow.com/questions/26566799/selenium-python-how-to-wait-until-the-page-is-loaded
    delay = 90 # seconds
    try:
        element_present = EC.presence_of_element_located((By.CLASS_NAME, 'bc-datatable')) # note : (( )) !!
        WebDriverWait(driver, delay).until(element_present)
        print("Page is ready!")
    except TimeoutException:
        print("Loading took too much time!")

    # time.sleep(25)

    html = driver.page_source.encode("utf-8")
    #html = ur.urlopen(url).read()           # obsolete
    soup = BeautifulSoup(html, "lxml")
    contracts = []
    table = soup.find("div", attrs={"class" : "column-inner"})
    #rows = table.findAll('tr')
    tables = table.findAll('div', attrs={"class" : "barchart-content-block"})
    headers = ["Symbol","Contract","Last","Changes","Open","High","Low","Volume","Open Interest","Time"]

    print('parsing html tables for {0}'.format(_exch_smb))
    for table in tables:
        
        # product title header 
        #table_title = table.find("div", attrs={"class" : "block-title joined"})
        #contracts.append([table_title.text.replace('\n',''),'','','','','','','','',''])

        # get all lines in the table
        lines = table.findAll("tr", attrs={"sly-repeat" : "row in rows.data"})

        # loop each field of the line
        for line in lines:
            fields = line.findAll("td")
            contracts.append([field.text.replace('\n','') for field in fields][:-1]) # the last field we don't need it!
          
    print('generating DataFrame ... {0}'.format(_exch_smb))
    df = pd.DataFrame(contracts, columns=headers)
    #g_contract_data_frame.append(df)
    #g_outfile.append(_outfile)
    #g_exch_smb.append(_exch_smb)
    print('converting DataFrame to Excel ... {0}'.format(_exch_smb))
    dataframe_to_excel(df, _outfile, _exch_smb)    
    #driver.close()    
    #global total_num_of_open_browser    
    #total_num_of_open_browser = total_num_of_open_browser - 1

    driver.close()

#print(ticker," ",url)
def get_barchart_table(ticker, url, _today):
    
    outfile = 'C:/actives/Barchart/barchart_{0}_{1}.xlsx'.format(ticker,_today)

    if '?' in ticker or len(ticker)==0 or not 'http' in url: # skip un-known tickers
        #continue
        #exit()
        pass
    # skip download price table if the file with records exists (more than 6KB in size)
    if not (iou.file_exist(outfile) and osu.get_file_size(outfile) > 3000):
        get_contracts_list_by_exch_from_barchart(outfile, url, ticker)

def get_and_save_futures_market_symbol_list(_today):

    print('Inside of 「get_and_save_futures_market_symbol_list」')

    url_col = 0
    eSignal_ticker_col = 1
    is_active_ticker_col = 3
    eSignal_active_ticker_list = []    
    barchart_download_url_list = []

    MAX_CONCURRENT_TREAD_NUM = 5
    
    #get_contracts_list_by_exch_from_barchart('test.csv', 'nymex')
    #driver = webdriver.Chrome('D:/WebDriver/chromedriver.exe') # webdriver.PhantomJS('./phantomjs-2.1.1-windows/bin/phantomjs.exe') # http://zipsan.hatenablog.jp/entry/20150413/1428861548 # webdriver.Chrome('./chromedriver.exe') #        

    import xlrd
    
    if iou.file_exist("G:/"): # Linux machine
        book = xlrd.open_workbook("G:\中心限月\BarchartPriceListUrls.xlsx")
    else: # Window emergency backup machine
        book = xlrd.open_workbook("C:/actives/BarchartPriceListUrls.xlsx")

    for name in book.sheet_names():
        sheet = book.sheet_by_name(name)
        for row in range(2, sheet.nrows):
            if('y' in sheet.cell(row, is_active_ticker_col).value): # only for active tickers
                eSignal_active_ticker_list.append(sheet.cell(row, eSignal_ticker_col).value)
                barchart_download_url_list.append(sheet.cell(row, url_col).value)
    
    # Create Excel file
    print('Downlading price list by ticker from Barchert.com - {0}'.format(_today))
    # Append excel sheets on it    
    m_thread = []
    total_num_of_open_browser = 0;

    thread_pool = []
    active_ticker_eSignal = []
    for ticker, url in zip(eSignal_active_ticker_list, barchart_download_url_list):
    
        if url == '': # eSignal has this ticker but Barchart doesn't seem to.
            continue

        uticker = ticker # universal ticker with @@ dummy mark
        ticker = ticker.replace(' @@','')
        sheet_name = ticker
        
        # Chrome暴走防止トリガー
        #while osu.is_process_with_cmdline_exist("Chrome") >= MAX_CONCURRENT_TREAD_NUM:
        #    import time
        #    time.sleep(10)   # wait for 10 sec

        t = threading.Thread(target=get_barchart_table, args=[ticker, url, _today], name='{0}|{1}|{2}'.format(ticker, uticker, sheet_name))
        thread_pool.append(t)
        t.start()

        # keep concurrent sub-thread number to at most 3

        if len(thread_pool) >= MAX_CONCURRENT_TREAD_NUM:
        
            thread_pool[0].join()
            thread_name = thread_pool[0].getName()
            thread_ticker = thread_name.split('|')[0]
            thread_uticker = thread_name.split('|')[1]
            thread_sheet_name = thread_name.split('|')[2]
            #active_ticker_eSignal.extend(find_active_month_from_symbol_list_table('G:/中心限月/Barchart/barchart_{0}_{1}.xlsx'.format(thread_ticker, _today), thread_sheet_name, thread_uticker))
            active_ticker_eSignal.extend(find_active_month_from_symbol_list_table('C:/actives/Barchart/barchart_{0}_{1}.xlsx'.format(thread_ticker, _today), thread_sheet_name, thread_uticker))
            active_ticker_eSignal.extend([thread_uticker.replace('@@', "#OI")]) # open interest ticker
            thread_pool.pop(0)

    #driver.close()

    for worker in thread_pool:
        print('joining {0}'.format(worker.getName()))
        worker.join()        

    return active_ticker_eSignal #sum(active_ticker_eSignal, []) # flaten LoL to list

def convert_MMYY_to_ticker_monthyear(_mmyy, _uticker):    

    mmyy =  _mmyy.replace('(','').replace(')','').replace('\'17','7').replace('\'','').replace('Jan','F').replace('Feb','G').replace('Mar','H').replace('Apr','J').replace('May','K').replace('Jun','M').replace('Jul','N').replace('Aug','Q').replace('Sep','U').replace('Oct','V').replace('Nov','X').replace('Dec','Z')
    if '-ICE' in _uticker or '-SGX' in _uticker or '-SGC' in _uticker:
        mmyy = mmyy[1:] + mmyy[:1] # V7 -> 7V
    return mmyy

def find_active_month_from_symbol_list_table(_excel_name, _sheet_name, _uticker):
    
    print('finding active months @ excelsheet',_sheet_name)
    
    book = _excel_name
    sheet= _sheet_name              # ??????V?[?g??
    try:
        excel  = pd.ExcelFile(book, encoding='utf-8')     # xlsx?t?@?C????Python???J??
    except:
        return []

    df = excel.parse(sheet)         # opened as dataframe               

    # get rid records with NaN in the Volume column
    df = df[pd.notnull(df['Volume'])]
    # convert number string with thousand's comma to float : http://stackoverflow.com/questions/33692532/pandas-error-can-only-use-str-accessor-with-string-values
    df['Volume'] = df['Volume'].astype(str).str.replace(',', '').astype(float).fillna(0.0) # this will fail if df['Volume'][i] is not a str type    

    # get the 3 most active months
    active_ticker_barchart = df.sort_values(by=["Volume","Contract"], ascending=False) if '-TCM' in _sheet_name else df.sort_values(by=["Volume","Contract"], ascending=False)[:3]
    (active_ticker_barchart[active_ticker_barchart.Volume >= 1000])

    active_ticker_eSignal = [_uticker.replace('@@', convert_MMYY_to_ticker_monthyear(x.replace('(FOB)','').replace(' \'','\'').split(" (")[1], _uticker)) for x in active_ticker_barchart['Contract']]
    
    return active_ticker_eSignal   # only get contracts whose volume exceeds 1000

def get_contract_of_FGHJKMNQUVXZ(_product): # VX, FCE
    yr =  datetime.datetime.now().year
    mth = datetime.datetime.now().month
    dict = {
        1:'F', 2:'G', 3:'H', 4:'J', 5:'K', 6:'M',
        7:'N', 8:'Q', 9:'U', 10:'V', 11:'X', 12:'Z',
    }
    this_yr_str = str(yr)[-1] if yr <= 17 else str(yr)
    next_yr_str = str(yr+1)[-1] if yr+1 <= 17 else str(yr+1)
    first = dict[mth] + this_yr_str
    second = dict[mth + 1] + this_yr_str if mth+1 <= 12 else dict[(mth + 1) % 12] + next_yr_str
    third = dict[mth + 2] + this_yr_str if mth+2 <= 12 else dict[(mth + 2) % 12] + next_yr_str

    first = _product.replace('@@', first)
    second = _product.replace('@@', second)
    third = _product.replace('@@', third)

    return [first, second, third]

def get_contract_of_HMUZ(_product):         # YM, FDAX
    yr =  datetime.datetime.now().year
    mth = datetime.datetime.now().month
    if int((mth - 1) / 3) == 0:
        first = 'H' + str(yr)[-1]
        second = 'M' + str(yr)[-1]
    elif int((mth - 1) / 3) == 1:
        first = 'M' + str(yr)[-1]
        second = 'U' + str(yr)[-1]
    elif int((mth - 1) / 3) == 2:
        first = 'U' + str(yr)[-1]
        second = 'Z' + str(yr)[-1]
    elif int((mth - 1) / 3) == 3:
        first = 'Z' + str(yr)[-1]
        second = 'H' + str(yr + 1)[-1]

    first = _product.replace('@@', first)
    second = _product.replace('@@', second)

    return [first, second]

def get_contract_continuous_month(_product):    # DX
    return [_product.replace('@@', '#F')]

def get_contract_volatility(_product):
    return [_product.replace('@@', '#V')]

def get_contract_oi(_product):
    return [_product.replace('@@', '#OI')]

#! -- not in use --
def get_expire_calender_from_barchart(_url):
    import codecs
    import re
    if "http://" in _url:
        html = ur.urlopen(_url).read()           # global site
    else:
        html = codecs.open(_url, 'rb', 'utf-8')  # local file
    soup = BeautifulSoup(html, "lxml")
    #header = soup.find_all("td", attrs={"class" : re.compile("bcTH")})
    contracts = soup.find_all("tr", attrs={"class" : re.compile("^bc")})
    from collections import defaultdict
    months = []
    dict = defaultdict(list)
    ric = ''
    for x in contracts:
        for y in x.text.split():
            if y:
                pattern = re.compile('\d{2}/\d{2}')
                if pattern.match(y):
                    months.append(y)
                else:
                    if len(months) > 0:
                        ric = ric[1:]   # remove the white space at the first character
                        dict[ric] = months
                        ric = ''
                        months = []
                    ric = " ".join([ric, y])
    return dict

def get_active_month_from_barchart(_url):
    import codecs
    import re
    if "http://" in _url:
        html = ur.urlopen(_url).read()           # global site
    else:
        html = codecs.open(_url, 'rb', 'utf-8')  # local file
    soup = BeautifulSoup(html, "lxml")
    #header = soup.find_all("td", attrs={"class" : re.compile("bcTH")})
    contracts = soup.find_all("tr", attrs={"id" : re.compile("^dt2")})
    ric_list = []
    for x in contracts:
        if "column" not in x:
            y = x.attrs['id'].replace("dt2_","")
            ric_list.append(y)
            print(y)
    return ric_list

#get_expire_calender_from_barchart('C:/Users/operator/Desktop/Futures - Expiration Calendar.html)
#get_active_month_from_barchart('C:/Users/operator/Desktop/Futures - Most Active Futures.html')

if __name__ == '__main__':
    
    start_now = datetime.datetime.now()

    from inspect import currentframe, getframeinfo
    frameinfo = getframeinfo(currentframe())
    this_file_name = frameinfo.filename[frameinfo.filename.rfind('\\')+1:]

    barchart_flag = False
    # look 7 days back in case some files missing
    for lookbackdays in [0]:

        # =========================== Today's Date ==============================
        from datetime import timedelta # http://stackoverflow.com/questions/6871016/adding-5-days-to-a-date-in-python
        today = dpu.date_to_str(datetime.date.today() + timedelta(days=lookbackdays)) #'2016-05-28'
        year = today.split('-')[0];
        month = today.split('-')[1].split('-')[0];
        day = today.split('-')[-1]
        if not dtu.is_weekday(year, month, day):
            None
            #exit(0)

        # ====================== TOCOM Daily Actives ==============================
        if iou.file_exist('G:/中心限月/tocom/{0}.csv'.format(today)):
            save_file = 'G:/中心限月/tocom/{0}.csv'.format(today)
        else:
            save_file = 'C:/中心限月/tocom/{0}.csv'.format(today)
        #GetTocomActiveMonth(save_file, int(year), int(month))
        if not iou.file_exist(save_file):    # ファイルが既にあれば、本日の作業は完了しているので、再取得はしない。
            tocom_thread = threading.Thread(target=GetTocomActiveMonth, args=[save_file, int(year), int(month)],name="thread Tocom Actives ...")
            tocom_thread.start()
            # 後続が時間かかるので、tocom_thread は join() するまでもないかな.

        if not barchart_flag: # only for today
            barchart_flag = True

            # ======== reorganizing barchart .xlsx tables ========
            osu.set_console_title(dtu.get_currnet_datetime_str())
            print('======== reorganizing barchart .xlsx tables ========')
            today_es_active_file = 'C:/actives/eSignal/es_actives_list_{0}.csv'.format(today)
            #if iou.file_exist(today_es_active_file):    # ファイルが既にあれば、本日の作業は完了しているので、再取得はしない。
            #    exit(0)
            base_folder = 'C:/actives/Barchart/' #'G:/中心限月/Barchart/'
            xlsx_list = iou.getfiles(base_folder,'{0}.xlsx'.format(today))
            #if len(xlsx_list)==180:                      # 180ファイルが既にあれば、本日の作業は完了しているので、再取得はしない。
            #    exit(0)
            for xlsx_file in iou.getfiles(base_folder,'.xlsx'):

                # 日付部分抽出
                try:
                    iso_dt = xlsx_file.split('_')[2].replace('.xlsx','')
                except:
                    print('exception @ moving xlsx',xlsx_file)
                    continue
                if iso_dt != today:
                    # ファイル移動
                    print(xlsx_file,' を移動中...')
                    org_dir = base_folder
                    dst_dir = base_folder+iso_dt+'/'
                    osu.create_folder_if_not_exist(dst_dir)
                    if not today in xlsx_file:
                        iou.move_file(org_dir + xlsx_file, dst_dir + xlsx_file, True)

            # ======================= Exchanges Daily Actives ===========================
            # ----------------------- Save Bartchart Tables -------------------------
            print(' ============================ get_and_save_futures_symbol_list ============================== ')
            eSignal_actives_list = get_and_save_futures_market_symbol_list(today) #if not iou.file_exist('barchart_smb_{0}.xlsx'.format(today)) else None

            # ==================== actives not-in-barchart page ==========================
            eSignal_actives_list.extend(get_contract_of_HMUZ('NI225_M @@-OSM'))          # OSAKA Nikkei225 Index mini futures
            eSignal_actives_list.extend(get_contract_of_HMUZ('NI225 @@-OSM'))            # OSAKA Nikkei225 Index futures        
            eSignal_actives_list.extend(get_contract_of_HMUZ('TOPIX @@-OSM'))            # OSAKA TOPIX futures
            eSignal_actives_list.extend(get_contract_of_HMUZ('JGBL @@-JGB'))             # OSAKA JGB 10YR
            eSignal_actives_list.extend(get_contract_of_HMUZ('JN400 @@-OSM'))            # OSAKA Nikkei400 Index futures
            eSignal_actives_list.extend(get_contract_of_HMUZ('TOPIXM @@-OSM'))           # OSAKA TOPIX mini futures
            eSignal_actives_list.extend(get_contract_of_HMUZ('FDAX @@-EUX'))             # DAX futures
            eSignal_actives_list.extend(get_contract_of_HMUZ('TF @@'))                   # Russel 2000 mini futures
            eSignal_actives_list.extend(get_contract_of_FGHJKMNQUVXZ('NIFTY @@-NSF'))    # NSE CNX NIFTY Index futures
            eSignal_actives_list.extend(get_contract_of_FGHJKMNQUVXZ('CNXIT @@-NSF  '))  # NSE CNX IT INDEX futures
            # TO ADD TFEX, SGX, HKFE. BMD, KFE individual stock futures             
            #'TRIGO B.A.'
            #'MAIZ ROS'
            #'SOJA ROS'

            eSignal_actives_list = [x.rstrip() for x in eSignal_actives_list] # remove right side spaces
            eSignal_actives_list = list(filter(None, eSignal_actives_list)) # fastest way remove empty entry : https://stackoverflow.com/questions/3845423/remove-empty-strings-from-a-list-of-strings
            pd.DataFrame(eSignal_actives_list).to_csv(today_es_active_file,index=False,header=False)
    

    body_text = 'Operation started at {0}'.format(start_now)
    wbu.send_gmail('中心限月 完了通知【{0}】'.format(dtu.get_currnet_datetime_str()), body_text)
    print('Leaving ... {0} main thread ... !'.format(frameinfo.filename))

    # copy it over to itubuntu04
    if iou.file_exist("G:/".format(today)): # Linux machine
        iou.create_folder_if_not_exist("G:/中心限月/eSignal/")
        osu.copy_file(today_es_active_file, "G:/中心限月/eSignal/")
    else: # Window emergency backup machine
        iou.create_folder_if_not_exist("C:/中心限月/eSignal/")
        osu.copy_file(today_es_active_file, "C:/中心限月/eSignal/")

    import win32com.client
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys("{ENTER}", 0) 