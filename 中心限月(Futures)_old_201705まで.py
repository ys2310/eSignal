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
sys.path.append('../PythonUtility/')

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

#const char* sDSN = "STEVE-PC";
#const char* sDatabase = "eS_201512";
#const char* sUserId = "sa";
#const char* sPassword = "Bigdata01";

#const char* sTable = "ARCA";*/

#const char* sODBC_DSN = "eSignal_201512";
#const char* sODBC_UserId = "sa";
#const char* sODBC_Password = "Bigdata01";

DSN = 'P11013'
Database = 'DWConfiguration'
UserId = 'sa'
Password = 'Bigdata01'
Table = 'tick'
odbcDSN = 'P11013-D'
odbcUserId = 'sa'
odbcPassword = 'Bigdata01'
CsvFolder = './ticks'

#dbu.bulkcopy_es_daily(DSN, Database, UserId, Password, Table, odbcDSN, odbcUserId, odbcPassword, CsvFolder)
#dbu.bulkcopy_es_tick(DSN, Database, UserId, Password, Table, odbcDSN, odbcUserId, odbcPassword, CsvFolder)
#dbu.bulkcopy_es_smmry(DSN, Database, UserId, Password, Table, odbcDSN, odbcUserId, odbcPassword, CsvFolder)

#total_num_of_open_browser = 0

# actievely traded contracts and we'll monitor them regardless of thier previous day's volume
es_essentials = [
    # SHFE - to add IMCI A0-SHF Nonferrous Metals Index
        'SHFE ALUMINIUM',
        'SHFE COPPER',
        'SHFE ZINC',
        'SHFE GOLD',
        'SHFE SILVER',
        'SHFE RUBBER',
        'SHFE STEEL REBAR',
        'SHFE LEAD',
        'SHFE NICKEL',
        'SHFE TIN',
        'SHFE BITUMEN',
        'SHFE HOT-ROLLED COIL',
        # DGCX
        'DGCX INDIAN RUPEE',
        # HKFE to add DHH - HSCEI Dividend Point
        'HANG SENG INDEX',
        'MINI HANG SENG INDEX',
        'H-SHARES INDEX',
        'MINI H-SHARES INDEX',
        # LME
        # ''
        # MDEX 
        # ''
        # TFEX - to add Individual Stock Futures
        'THAI SET50 INDEX',
        # TAIWA
        # 'TAIWAN WEIGHTED IDX'
        # KFE
        # ''
        # DME
        'DME CRUDE OIL OMAN',
        # DCE
        'DCE NO.2 SOYBEANS',
        'DCE NO.1 SOYBEANS',
        'DCE SOY MEAL',
        'DCE CORN',
        'DCE CORN STARCH',
        'DCE SOYBEAN OIL',
        'DCE POLYETHYLENE',
        'DCE POLYPROPYLENE',
        'DCE PALM OLEIN',
        'DCE PVC',
        'DCE COKE',
        'DCE COKING COAL',
        'DCE IRON ORE',
        'DCE FRESH HEN EGG',
        'DCE FIBERBOARD',
        'DCE BLOCKBOARD',
        # CZCE
        'CZCE PURE TERE ACID',
        'CZCE SUGAR WHITE',
        'CZCE COTTON NO.1',
        'CZCE METHANOL',
        'CZCE RAPESEED OIL',
        'CZCE FLAT GLASS',
        'CZCE RAPESEED MEAL',
        'CZCE THERMAL COAL100',
        # NCDEX
        #'NCDEX CHANA',
        'NCDEX SOYBEANS',
        'NCDEX SOYBEAN OIL',
        #'NCDEX RAPE MUSTARD'
        'NCDEX CORIANDER',
        'NCDEX JEERA',
        'NCDEX TURMERIC',
        'NCDEX KAPAS V 797',
        'NCDEX GUARSEED',
        'NCDEX GUARGUM',
        # OSAKA
        'NIKKEI 225',
        'NIKKEI 225 MINI',
        'NIKKEI 400',
        'TOKYO TOPIX INDEX',
        'TOKYO TOPIX-MINI',
        'JPX 10-YEAR JGB',
        # NSE
        'NSE CNX NIFTY',
        'NSE CNX IT INDEX',
        'NSE BANK NIFTY INDEX',
        # CFE
        'S&P 500 VIX',
        # MGEX
        'SPRING WHEAT',
        # MNTRL        
        'CANADIAN 10-YEAR',
        'CANADIAN BA',
        'S&P TSX 60',
        # OMX
        'OMX SWEDISH INDEX',
        # BMF
        '1-DAY INTERBANK DEP.',
        'BMF LIVE CATTLE',
        'BMF US DOLLAR',
        'BMF BOVESPA INDEX',
        'BMF BOVESPA MINI',
        # MATBA
        #'TRIGO B.A.'
        #'MAIZ ROS'
        #'SOJA ROS'
        # TOCOM
        'TOCOM CRUDE OIL',
        'TOCOM GOLD',
        'TOCOM PLATINUM',
        'TOCOM GASOLINE',
        'TOCOM KEROSENE',
        'TOCOM RUBBER',
        'TOCOM PALLADIUM',
        'TOCOM SILVER',
        'TOCOM CORN',
        'TOCOM U.S. SOYBEANS',
        # SAFEX
        'JSE FTSE TOP 40',
        # ''
        'JSE FTSE WEIGHTED 40',
        'SAFEX WHITE MAIZE',
        # ICE-USA
        'COFFEE',
        'COCOA',
        'SUGAR #11',
        'COTTON #2',
        # ICE-EU
        'COCOA #7',
        'ROBUSTA COFFEE 10-T',
        'WHITE SUGAR #5', 
        # ICE-IND 
        'RUSSELL 2000 MINI',
        'MSCI EMI INDEX',
        # ICE
        'CRUDE OIL BRENT',
        'CRUDE OIL WTI ICE',
        'ICE GAS OIL LS',
        'ICE NATURAL GAS',
        'ICE RBOB BLENDSTOCK',
        'ICE HEATING OIL',
        'ICE EUA FUTURES',
        # LIF-IND
        'FTSE 100',
        # LIFFFE
        '3-MONTH EURIBOR',
        '3-MONTH STERLING',
        '3-MONTH EUROSWISS',
        # WPG
        'CANOLA',
        # COMEX
        'HIGH GRADE COPPER',
        'GOLD',
        'SILVER',
        # NYMEX
        'CRUDE OIL WTI',
        'ULSD NY HARBOR',
        'GASOLINE RBOB',
        'NATURAL GAS',
        'PLATINUM',
        'PALLADIUM',
        # CME
        'FEEDER CATTLE',
        'LIVE CATTLE',
        'LEAN HOGS',
        # IMM-CUR
        'AUSTRALIAN DOLLAR',
        'BRITISH POUND',
        'CANADIAN DOLLAR',
        'JAPANESE YEN',
        'EURO FX',
        'SWISS FRANC',
        'MEXICAN PESO',
        'BRAZILIAN REAL',
        'NEW ZEALAND DOLLAR',
        'RUSSIAN RUBLE',
        'SOUTH AFRICAN RAND',
        'EURO/POUND',
        'EURO/YEN',
        'EURO/SWISS',
        # IOM
        'S&P 500 INDEX',
        'NIKKEI 225 YEN',
        # IMM-FIN
        'EURODOLLAR',
        # GLBX
        'E-MINI S&P 500',
        'E-MINI S&P MIDCAP',
        'E-MINI NASDAQ 100',
        # 'E-MINI DOW' 
        'E-MINI JAPANESE YEN',
        'E-MINI EURO FX',
        'E-MICRO GBP/USD',
        'E-MICRO EUR/USD',
        'E-MICRO AUD/USD',
        # CBOT-AGR
        'CORN',
        'WHEAT',
        'HARD RED WHEAT',
        'SOYBEANS',
        'SOYBEAN MEAL',
        'SOYBEAN OIL',
        # CBOT-FIN
        'T-BOND',
        '10-YEAR T-NOTE',
        '5-YEAR T-NOTE',
        '3-YEAR T-NOTE',
        '2-YEAR T-NOTE',
        'ULTRA T-BOND',
        'ULTRA 10-YEAR T-NOTE',
        '30-DAY FED FUNDS',
        # CBOT-M
        'DJIA MINI-SIZED',
        # EUR-IND
        'STOXX EUROPE 600',
        'EURO STOXX DIVIDEND',
        'VSTOXX MINI'
        #'STOXX 50 ? ' 
        #'DAX ? ' 
        # EUREX
        'EURO BUND',
        'EURO BOBL',
        'EURO SCHATZ',
        'EURO BUXL',
        'EURO OAT LONG-TERM',
        'EURO BTP LONG-TERM',
        'EURO BTP SHORT-TERM',
]

def GetTocomActiveMonth(_save_file, _year, _month):
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

def GetEurexActiveMonth():

    EurexTicker = [ "FBTP", "FBTS", "FGBL", "FGBM", "FGBS", "FGBX", "FOAT", "FESX", "FDAX"]
    EurexProductCode = [ "16140", "16006", "14772", "15646", "7226", "15204", "154588", "18956", "17208"]

    dateStr = '20160524'
    for x in EurexProductCode:
        page = "http://www.eurexchange.com/exchange-en/market-data/statistics/market-statistics-online/180102!onlineStats?viewType=4&productGroupId=862&productId=" + x + "&cp=&month=&year=&busDate=" + dateStr
        html = wb_util.get_html(page)
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", attrs={"class" : "dataTable"})
        grids = table.contents[1].contents
        g_list = []
        for g in grids:
            g_list.append([td.text for td in g.find_all("td") if len(td) > 0])

        max_vol = 0
        active_month = ''
        for x in g_list:              
            volume = int(x[6].replace(",",""))
            if volume > max_vol and x[0] != 'Total':
                max_vol = volume
                active_month = x[0]

        print(" == ")

def GetIceActiveMonth():
    d

def convert_barchart_name_to_es_name(_name):
    dict = {
    'Wheat' : 'ZW', 'Corn' : 'ZC', 'Soybeans' : 'ZS', 'Soybean Meal' : 'ZM', 'Soybean Oil' : 'ZL',
    'Gold' : 'GC', 'Silver' : 'SI', 'High Grade Copper' : 'HG', 'Platinum' : 'PL', 'Palladium' : 'PA',
    'U.S. Doller Index' : 'DX', 'British Pound' : '6B', 'Canadian Dollar' : '6C', 'Japanese Yen' : '6J', 'Swiss Franc' : '6S', 'Euro FX' : '6E', 'Australian Dollar' : '6A', 'Mexican Peso' : '6M', 'New Zealand Dollar' : '6N', 'South Africa Rand' : '6Z', 'Brazilian Real' : '6B', 'Russian Ruble' : '6R',
    'Crude Oil' : 'CL', 'Heating Oil' : 'HO', 'Gasoline RBOB' : 'XRB', 'Natural Gas' : 'NG', 'Brent Crude Oil' : 'BRN XX-ICE',
    'T-Bond' : 'ZW', 'Ultra T-Bond' : 'ZC', '10 Year T-Note' : 'ZS', '5 Year T-Note' : 'ZM', '2 Year T-Note' : 'ZL', '30 Day Fed Funds' : '', 'Eurodollar' : '',
    'Live Cattle' : 'LE', 'Lean hogs' : 'HE', 
    'Cotton #2' : 'CT', 'Coffee' : 'KC', 'Sugar #11' : 'SB', 'Cocoa' : 'CC', 
    'Mini S&P 500 Index' : 'ES', 'E-Mini Nasdaq' : 'NQ', 'Mini-Sized Dow' : 'YM', 'E-Mini Russell 2000' : 'ZM', 'E-Mini S&P Midcap' : 'ZL', 'S&P 500 VIX' : '', 'GSCI' : ''
    }

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
    print('Generating excel file {0}'.format(_excel_name))
    _dataframe.to_excel(_excel_name, sheet_name=_sheet_name, index=False)
    #_dataframe.to_csv(_excel_name, index=False)

#g_contract_data_frame = []
#g_outfile = []
#g_exch_smb = []
def get_contracts_list_by_exch_from_barchart(_driver, _outfile, _exch_smb):

    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By

    #url = 'http://www.barchart.com/futures/price-by-exchange/' + _exch_smb
    
    url = 'https://www.barchart.com/futures/prices-by-exchange/' + _exch_smb
    print(url)

	# use SELENIUM to select next menu (iBoxx index)
    print('Openning webdriver ... {0}'.format(_exch_smb))
    driver = _driver #webdriver.Chrome('D:/WebDriver/chromedriver.exe') # webdriver.PhantomJS('./phantomjs-2.1.1-windows/bin/phantomjs.exe') # http://zipsan.hatenablog.jp/entry/20150413/1428861548 # webdriver.Chrome('./chromedriver.exe') #        
    print('Getting url ... {0}'.format(_exch_smb))
    driver.get(url)

    # wait till the page loads : http://stackoverflow.com/questions/26566799/selenium-python-how-to-wait-until-the-page-is-loaded
    delay = 60 # seconds
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
    headers = ["Contract","Symbol","Last","Changes","Open","High","Low","Volume","Open Interest","Time"]

    print('parsing html tables for {0}'.format(_exch_smb))
    for table in tables:
        
        # product title header 
        table_title = table.find("div", attrs={"class" : "block-title joined"})
        contracts.append([table_title.text.replace('\n',''),'','','','','','','','',''])

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

def get_and_save_futures_market_symbol_list(_today):

    #get_contracts_list_by_exch_from_barchart('test.csv', 'nymex')

    exch_smb = [
        'shfe', 'dgcx', 'hkfe', 'tfex', 'taiwa', 'dme', 'dce', 'czce', 'ncdex', 'osaka', #'nse',
        'cfe', 'mgex', 'mntrl', 'omx', 'bmf', 'tocom', 'safex', #'matba',
        #'ice-usa', 'ice-ind', 'ice', 'lif-ind', 'liffe', 'wpg', 'comex', 'nymex', 'cme', 'imm-cur',
        #'iom', 'imm-fin', 'gblx', 'cbot-agr', 'cbot-fin', 'cbotm', 'eur-ind', 'eurex'
        'ice-us-commodities', 'ice-us-indices', 'ice-us-currencies',
        'ice-eu-commodities', 'ice-eu-indices', 'ice-eu-energies', 'ice-eu-interest-rates',
        'ice-ca', 'comex-metals', 
        'nymex-energies', #'nymex-softs',                     
        'cme-ag','cme-currencies', 'cme-indices', 'cme-interest-rates', 'cme-mini', # 'cme-housing',
        'cbot-ag', 'cbot-interest-rates', # , 'cbot-indices'
        'eurex-indices', 'eurex-interest-rates',                   
        # 'lme', 'mdex', 'kfe'
    ]

    #[get_contracts_list_by_exch_from_barchart(outfile, x) for x in exch_smb]
    # Create Excel file
    print('Creating symbol Excel sheet for {0}'.format(_today))
    # Append excel sheets on it    
    m_thread = []
    total_num_of_open_browser = 0;

    print('Appending excel sheets')
    driver = webdriver.Chrome('D:/WebDriver/chromedriver.exe') # webdriver.PhantomJS('./phantomjs-2.1.1-windows/bin/phantomjs.exe') # http://zipsan.hatenablog.jp/entry/20150413/1428861548 # webdriver.Chrome('./chromedriver.exe') #        
    for i,smb in enumerate(exch_smb):
        outfile = 'G:/中心限月/Barchart/barchart_{0}_{1}.xlsx'.format(smb,_today)
        #os.remove(outfile) if os.path.exists(outfile) else None
        #if not iou.file_exist(outfile):            
        #t = threading.Thread(target=get_contracts_list_by_exch_from_barchart, args=[outfile,smb], name="thread{0}".format(i))
        #m_thread.append(t)
        #t.start()

        # if the file with records exists (more than 6KB in size)
        if iou.file_exist(outfile) and osu.get_file_size(outfile) > 6000:
            continue
        
        get_contracts_list_by_exch_from_barchart(driver, outfile, smb)

        # using multithreading cause 'Python has stopped working!' problem...
        # 
        #get_contracts_list_by_exch_from_barchart(outfile,smb)
        #t1 = threading.Thread(target=get_contracts_list_by_exch_from_barchart, args=[outfile,smb],name="thread {0}".format(i))
        #m_thread.append(t1)
        #t1.start()
        #global total_num_of_open_browser
        #total_num_of_open_browser = total_num_of_open_browser + 1
        
        # execute thread every 30 secs
        # opening multiple webdriver at the same time will cause issue of "python.exe has stopped working" run-time error!
        #time.sleep(30 * total_num_of_open_browser)
        # limit the number of opening browser to be 3
        #while total_num_of_open_browser == 3:

        #    print('waiting 20 secs ')
        #    time.sleep(20)

        #else:

            #if i == 3 * j - 1: # execute every 3 threads 
            # join all threads at the end
    #for worker in m_thread:
    #    print('\n,joining {0}'.format(worker.getName()))
    #    total_num_of_open_browser = total_num_of_open_browser - 1
    #    worker.join()
             #   j = j + 1

    #print('')
    #for worker in m_thread:
    #    print('joining {0}'.format(worker.getName()))
    #    worker.join()

    #for x in range(len(g_contract_data_frame)):
    #    dataframe_to_excel(g_contract_data_frame[x], g_outfile[x], exch_smb[x])
    
    driver.close()

    print('done!')

def find_active_month_from_symbol_list_table(_excel_name, _sheet_name):
    
    print('finding active months @ excelsheet ',_sheet_name)

    book = _excel_name
    sheet= _sheet_name              # ??????V?[?g??
    excel  = pd.ExcelFile(book, encoding='utf-8')     # xlsx?t?@?C????Python???J??
    df = excel.parse(sheet)         # opened as dataframe

    import math
    header_rows = [i for i,line in enumerate(df.values) if (math.isnan(line[2]) if not isinstance(line[2], str) else False)]
    header_rows.append(len(df)) # add a dummy for easier later caculation    

    smb_col = []
    for i,j in zip(header_rows, header_rows[1:]):
        smb_col.append([df.values[i][0] for m in range(j-i)])
    # flatten list of lists to a list
    df['smb_col'] = sum(smb_col, [])
    # get rid records with NaN in the Volume column
    df = df[pd.notnull(df['Volume'])]
    # exclude the last empty col
    df = df[:-1]
    # convert number string with thousand's comma to float    
    # ref : http://stackoverflow.com/questions/33692532/pandas-error-can-only-use-str-accessor-with-string-values
    df['Volume'] = df['Volume'].astype(str).str.replace(',', '').astype(float).fillna(0.0) # this will fail if df['Volume'][i] is not a str type
    # subseting dataframe

    df = df[['Contract', 'Volume', 'smb_col']]
    # get the 3 most active months
    actives = df.sort_values(by=['smb_col', 'Volume'], ascending=False).groupby('smb_col').head(6) if _sheet_name == 'tocom' else df.sort_values(by=['smb_col', 'Volume'], ascending=False).groupby('smb_col').head(3)
    # only get contracts whose volume exceeds 1000
    #A = actives[actives.Volume >= 1000]

    # tolower case
    actives['smb_col'] = [x.lower() for x in actives['smb_col']]
    es_essentials_lowercase = [x.lower() for x in es_essentials]

    # return only the necessary list
    return actives[actives['smb_col'].isin(es_essentials_lowercase)] # ref : http://stackoverflow.com/questions/22542312/slice-pandas-dataframe-where-columns-value-exists-in-another-array
    
def get_open_interest_symbol():         
    return [
             'AL #OI-SHF', 'CU #OI-SHF', 'ZN #OI-SHF', 'AU #OI-SHF', 'AG #OI-SHF', 'RU #OI-SHF', 'RB #OI-SHF', 'PB #OI-SHF', 'NI #OI-SHF', 'SN #OI-SHF', 'BU #OI-SHF', 'HC #OI-SHF',
             '? #OI-DCX',		
             'HSI #OI-HKF', 'MHI #OI-HKF', 'HHI #OI-HKF', 'MCH #OI-HKF',  				
             'CAD #OI-LME', 'NID #OI-LME', 'ZSD #OI-LME', 'AHD #OI-LME', 'PBD #OI-LME', 
             'S50 #OI-TFE', 					
             'OQ #OI-DME', 								
             'B #OI-DCE', 'A #OI-DCE', 'M #OI-DCE', 'C #OI-DCE', 'CS #OI-DCE', 'Y #OI-DCE', 'L #OI-DCE', 'PP #OI-DCE', 'P #OI-DCE', 'V #OI-DCE', 'J #OI-DCE', 'JM #OI-DCE', 'I #OI-DCE', 'JD #OI-DCE', 'FB #OI-DCE', 'BB #OI-DCE', 				
             'TA #OI-ZCE', 'SR #OI-ZCE', 'CF #OI-ZCE', 'MA #OI-ZCE', 'OI #OI-ZCE', 'FG #OI-ZCE', 'RM #OI-ZCE', 'ZC #OI-ZCE', 								
             'CHARJDDEL #OI-NCD', 'SYBEANIDR #OI-NCD', 'SYOREF #OI-NCD', 
             'DHANIYA #OI-NCD', 'JEERAUNJHA #OI-NCD', 'TMCFGRNZM #OI-NCD', 'KAPASSRNR #OI-NCD', 'GUARSEED10 #OI-NCD', 'GUARGUM #OI-NCD',								
             'NI225 #OI-OSM', 'NI225_M #OI-OSM', 'JN400 #OI-OSM', 'TOPIX #OI-OSM', 'TOPIXM #OI-OSM', 'JGBL #OI-JGB', 				
             'NIFTY #OI-NSF', 'CNXIT #OI-NSF', 'BANKNIFTY #OI-NSF', 			
             'VX #OI-CF', 
             'QMW #OI', 				
             'CGB #OI-MX', 'BAX #OI-MX', 'SXF #OI-MX', 
             'OMXS30 #OI-OMF', 
             'DI1 #OI-BMF', 'BGI #OI-BMF', 'DOL #OI-BMF', 'IND #OI-BMF', 'WIN #OI-BMF', 					
             'TCL #OI-TCM', 'TGD #OI-TCM', 'TPL #OI-TCM', 'THU #OI-TCM', 'THO #OI-TCM', 'TRB #OI-TCM', 'TPA #OI-TCM', 'TSI #OI-TCM', 'CORN #OI-TGE', 'SOYB #OI-TGE',
             'ALSI #OI-SAF', 'ALMI #OI-SAF', 'DTOP #OI-SAF', 'WMAZ #OI-SAF',  				
             'KC #OI', 'CC #OI', 'SB #OI', 'CT #OI', 				
             # to add MFS, MMW, MWL, MFSB, MMWB				
             'TF #OI', 'MME #OI', 				
             'BRN #OI-ICE', 		
             'WBS #OI-ICE', 
             'GAS #OI-ICE', 				
             'GWM #OI-ICE',				
             'UHU #OI-ICE', 				
             'UHO #OI-ICE', 				
             'ECF #OI-ICE',								
             'Z #OI-ICL', 								
             'I #OI-ICL', 				
             'L #OI-ICL', 				
             'S #OI-ICL', 								
             'RS #OI-WC', 								
             'HG #OI', 				
             'GC #OI', 				
             'SI #OI', 								
             'CL #OI', 				
             'HO #OI', 				
             'RB #OI', 				
             'NG #OI', 				
             'PL #OI', 				
             'PA #OI', 								
             'FC #OI', 				
             'LE #OI', 				
             'HE #OI', 								
             '6A #OI', 				
             '6B #OI', 				
             '6C #OI', 				
             '6J #OI',				
             '6E #OI',				
             '6S #OI', 				
             '6M #OI', 				
             '6L #OI', 				
             '6N #OI', 				
             '6R #OI', 				
             '6Z #OI',				
             'RP #OI', 				
             'RY #OI', 				
             'RF #OI', 				
             'SP #OI', 				
             'NIY #OI',								
             'GE #OI',								
             'ES #OI', 				
             'MC #OI', 				
             'NQ #OI', 				
             'YM #OI',				
             'ZJ #OI', 				
             'ZE #OI', 				
             'M6B #OI', 				
             'M6E #OI', 				
             'M6A #OI',								
             'ZC #OI', 				
             'ZW #OI', 				
             'KE #OI', 				
             'ZS #OI', 				
             'ZM #OI', 				
             'ZL #OI', 								
             'ZB #OI', 				
             'ZN #OI', 				
             'ZF #OI', 				
             'ZE #OI', 				
             'ZT #OI', 				
             'UL #OI', 				
             'TN #OI', 				
             'ZQ #OI',								
             'YM #OI',								
             'FXXP #OI-EUX', 				
             'FEXD #OI-EUX', 				
             'FVS #OI-EUX',				
             'FESX #OI-EUX', 				
             'FDAX #OI-EUX', 								
             'FGBL #OI-EUX', 				
             'FGBM #OI-EUX', 				
             'FGBS #OI-EUX', 				
             'FGBX #OI-EUX', 				
             'FOAT #OI-EUX', 				
             'FBTP #OI-EUX', 				
             'FBTS #OI-EUX',
    ]

def barchart_tt_symbol_mapping(_actives):

    print('barchart to tt symbol mapping')

    dict = {
        # SHFE - to add IMCI A0-SHF Nonferrous Metals Index
        'SHFE ALUMINIUM': 'AL @@-SHF', 
        'SHFE COPPER': 'CU @@-SHF',         
        'SHFE ZINC': 'ZN @@-SHF', 
        'SHFE GOLD': 'AU @@-SHF', 
        'SHFE SILVER': 'AG @@-SHF', 
        'SHFE RUBBER': 'RU @@-SHF', 
        'SHFE STEEL REBAR': 'RB @@-SHF', 
        'SHFE LEAD': 'PB @@-SHF', 
        'SHFE NICKEL': 'NI @@-SHF', 
        'SHFE TIN': 'SN @@-SHF', 
        'SHFE BITUMEN': 'BU @@-SHF', 
        'SHFE HOT-ROLLED COIL': 'HC @@-SHF',
        # DGCX(Dubai Commodity Ex.)
        'DGCX INDIAN RUPEE': 'DINR @@-DGC',
        # HKFE to add DHH - HSCEI Dividend Point, CUS - RENMENBi CURRENCY
        'HANG SENG INDEX': 'HSI @@-HKF', 
        'MINI HANG SENG INDEX': 'MHI @@-HKF', 
        'H-SHARES INDEX': 'HHI @@-HKF', 
        'MINI H-SHARES INDEX': 'MCH @@-HKF',   
        # LME
        # '': 'CAD M3-LME', '': 'NID M3-LME', '': 'ZSD M3-LME', '': 'AHD M3-LME', '': 'PBD M3-LME', 
        # MDEX 
        # '': '', '': '', '': '', '': '', '': '', 
        # TFEX - to add Individual Stock Futures
        'THAI SET50 INDEX': 'S50 @@-TFE', 
        # TAIWA
        # 'TAIWAN WEIGHTED IDX': '', <- couldn't find on search tool
        # KFE
        # '': '', '': '', '': '',
        # DME
        'DME CRUDE OIL OMAN': 'OQ @@-DME', 
        # DCE
        #'DCE NO.2 SOYBEANS': 'B @@-DCE', 
        'DCE NO.1 SOYBEANS': 'A @@-DCE', 
        'DCE SOY MEAL': 'M @@-DCE', 
        'DCE CORN': 'C @@-DCE', 
        'DCE CORN STARCH': 'CS @@-DCE', 
        'DCE SOYBEAN OIL': 'Y @@-DCE', 
        'DCE POLYETHYLENE': 'L @@-DCE', 
        'DCE POLYPROPYLENE': 'PP @@-DCE', 
        'DCE PALM OLEIN': 'P @@-DCE', 
        'DCE PVC': 'V @@-DCE', 
        'DCE COKE': 'J @@-DCE', 
        'DCE COKING COAL': 'JM @@-DCE', 
        'DCE IRON ORE': 'I @@-DCE', 
        'DCE FRESH HEN EGG': 'JD @@-DCE', 
        'DCE FIBERBOARD': 'FB @@-DCE', 
        #'DCE BLOCKBOARD': 'BB @@-DCE',
        # CZCE
        'CZCE PURE TERE ACID': 'TA @@-ZCE', 
        'CZCE SUGAR WHITE': 'SR @@-ZCE', 
        'CZCE COTTON NO.1': 'CF @@-ZCE', 
        'CZCE METHANOL': 'MA @@-ZCE',         
        'CZCE RAPESEED OIL': 'OI @@-ZCE',         
        'CZCE FLAT GLASS': 'FG @@-ZCE',             
        'CZCE RAPESEED MEAL': 'RM @@-ZCE', 
        'CZCE THERMAL COAL100': 'ZC @@-ZCE', 
        # NCDEX
        'NCDEX CHANA': 'CHARJDDEL @@-NCD', 
        'NCDEX SOYBEANS': 'SYBEANIDR @@-NCD',
        'NCDEX SOYBEAN OIL': 'SYOREF @@-NCD', 
        #'NCDEX RAPE MUSTARD': '', ?
        'NCDEX CORIANDER': 'DHANIYA @@-NCD', 
        'NCDEX JEERA': 'JEERAUNJHA @@-NCD',         
        'NCDEX TURMERIC': 'TMCFGRNZM @@-NCD',        
        'NCDEX KAPAS V 797': 'KAPASSRNR @@-NCD',         
        'NCDEX GUARSEED': 'GUARSEED10 @@-NCD', 
        'NCDEX GUARGUM': 'GUARGUM @@-NCD',
        # OSAKA
        'NIKKEI 225': 'NI225 @@-OSM', 
        'NIKKEI 225 MINI': 'NI225_M @@-OSM', 
        'NIKKEI 400': 'JN400 @@-OSM', 
        'TOKYO TOPIX INDEX': 'TOPIX @@-OSM', 
        'TOKYO TOPIX-MINI': 'TOPIXM @@-OSM', 
        'JPX 10-YEAR JGB': 'JGBL @@-JGB',         
        # NSE
        'NSE CNX NIFTY': 'NIFTY @@-NSF', 
        'NSE CNX IT INDEX': 'CNXIT @@-NSF', 
        'NSE BANK NIFTY INDEX': 'BANKNIFTY @@-NSF', 
        # CFE
        'S&P 500 VIX': 'VX @@-CF', 
        # MGEX
        'SPRING WHEAT': 'QMW @@', 
        # MNTRL        
        'CANADIAN 10-YEAR': 'CGB @@-MX',       
        'CANADIAN BA': 'BAX @@-MX', 
        'S&P TSX 60': 'SXF @@-MX',  
        # OMX
        'OMX SWEDISH INDEX': 'OMXS30 @@-OMF', 
        # BMF
        '1-DAY INTERBANK DEP.': 'DI1 @@-BMF', 
        'BMF LIVE CATTLE': 'BGI @@-BMF', 
        'BMF US DOLLAR': 'DOL @@-BMF', 
        'BMF BOVESPA INDEX': 'IND @@-BMF', 
        'BMF BOVESPA MINI': 'WIN @@-BMF', 
        # MATBA
        #'TRIGO B.A.': '', 
        #'MAIZ ROS': '', 
        #'SOJA ROS': '', 
        # TOCOM
        'TOCOM CRUDE OIL': 'TCL @@-TCM', 
        'TOCOM GOLD': 'TGD @@-TCM', 
        'TOCOM PLATINUM': 'TPL @@-TCM', 
        'TOCOM GASOLINE': 'THU @@-TCM', 
        'TOCOM KEROSENE': 'THO @@-TCM', 
        'TOCOM RUBBER': 'TRB @@-TCM', 
        'TOCOM PALLADIUM': 'TPA @@-TCM', 
        'TOCOM SILVER': 'TSI @@-TCM', 
        'TOCOM CORN': 'CORN @@-TGE', 
        'TOCOM U.S. SOYBEANS': 'SOYB @@-TGE',
        # SAFEX
        'JSE FTSE TOP 40': 'ALSI @@-SAF', 
        # '': 'ALMI @@-SAF'
        'JSE FTSE WEIGHTED 40': 'DTOP @@-SAF', 
        'SAFEX WHITE MAIZE': 'WMAZ @@-SAF',          
        # ICE-USA
        'COFFEE': 'KC @@', 
        'COCOA': 'CC @@', 
        'SUGAR #11': 'SB @@', 
        'COTTON #2': 'CT @@', 
        # ICE-EU
        'COCOA #7': 'C @@-ICE', 
        'ROBUSTA COFFEE 10-T': 'RC @@-ICE', 
        'WHITE SUGAR #5': 'W @@-ICE', 
        # ICE-IND : to add MFS, MMW, MWL, MFSB, MMWB
        'RUSSELL 2000 MINI': 'TF @@', 
        'MSCI EMI INDEX': 'MME @@',         
        # ICE
        'CRUDE OIL BRENT': 'BRN @@-ICE', 
        'CRUDE OIL WTI ICE': 'WBS @@-ICE',         
        'ICE GAS OIL LS': 'GAS @@-ICE', 
        'ICE NATURAL GAS': 'GWM @@-ICE',
        'ICE RBOB BLENDSTOCK': 'UHU @@-ICE', 
        'ICE HEATING OIL': 'UHO @@-ICE', 
        'ICE EUA FUTURES': 'ECF @@-ICE',
        # LIF-IND
        'FTSE 100': 'Z @@-ICL', 
        # LIFFFE
        '3-MONTH EURIBOR': 'I @@-ICL', 
        '3-MONTH STERLING': 'L @@-ICL', 
        '3-MONTH EUROSWISS': 'S @@-ICL', 
        # WPG
        'CANOLA': 'RS @@-WC', 
        # COMEX
        'HIGH GRADE COPPER': 'HG @@', 
        'GOLD': 'GC @@', 
        'SILVER': 'SI @@', 
        # NYMEX
        'CRUDE OIL WTI': 'CL @@', 
        'ULSD NY HARBOR': 'HO @@', 
        'GASOLINE RBOB': 'RB @@', 
        'NATURAL GAS': 'NG @@', 
        'PLATINUM': 'PL @@', 
        'PALLADIUM': 'PA @@', 
        # CME
        'FEEDER CATTLE': 'FC @@', 
        'LIVE CATTLE': 'LE @@', 
        'LEAN HOGS': 'HE @@', 
        # IMM-CUR
        'AUSTRALIAN DOLLAR': '6A @@', 
        'BRITISH POUND': '6B @@', 
        'CANADIAN DOLLAR': '6C @@', 
        'JAPANESE YEN': '6J @@',
        'EURO FX': '6E @@',
        'SWISS FRANC': '6S @@', 
        'MEXICAN PESO': '6M @@', 
        'BRAZILIAN REAL': '6L @@', 
        'NEW ZEALAND DOLLAR': '6N @@', 
        'RUSSIAN RUBLE': '6R @@', 
        'SOUTH AFRICAN RAND': '6Z @@',
        'EURO/POUND': 'RP @@', 
        'EURO/YEN': 'RY @@', 
        'EURO/SWISS': 'RF @@',         
        # IOM
        'S&P 500 INDEX': 'SP @@', 
        'NIKKEI 225 YEN': 'NIY @@',
        # IMM-FIN
        'EURODOLLAR': 'GE @@',
        # GLBX
        'E-MINI S&P 500': 'ES @@', 
        'E-MINI S&P MIDCAP': 'MC @@', 
        'E-MINI NASDAQ 100': 'NQ @@', 
        # 'E-MINI DOW' : 'YM @@',
        'E-MINI JAPANESE YEN': 'ZJ @@', 
        'E-MINI EURO FX': 'ZE @@', 
        'E-MICRO GBP/USD': 'M6B @@', 
        'E-MICRO EUR/USD': 'M6E @@', 
        'E-MICRO AUD/USD': 'M6A @@',
        # CBOT-AGR
        'CORN': 'ZC @@', 
        'WHEAT': 'ZW @@', 
        'HARD RED WHEAT': 'KE @@', 
        'SOYBEANS': 'ZS @@', 
        'SOYBEAN MEAL': 'ZM @@', 
        'SOYBEAN OIL': 'ZL @@', 
        # CBOT-FIN
        'T-BOND': 'ZB @@', 
        '10-YEAR T-NOTE': 'ZN @@', 
        '5-YEAR T-NOTE': 'ZF @@', 
        '3-YEAR T-NOTE': 'ZE @@', 
        '2-YEAR T-NOTE': 'ZT @@', 
        'ULTRA T-BOND': 'UL @@', 
        'ULTRA 10-YEAR T-NOTE': 'TN @@', 
        '30-DAY FED FUNDS': 'ZQ @@',
        # CBOT-M
        'DJIA MINI-SIZED': 'YM @@',
        # EUR-IND
        'STOXX EUROPE 600': 'FXXP @@-EUX', 
        'EURO STOXX DIVIDEND': 'FEXD @@-EUX', 
        'VSTOXX MINI': 'FVS @@-EUX',
        #'STOXX 50 ? ' : 'FESX @@-EUX', 
        #'DAX ? ' : 'FDAX @@-EUX', 
        # EUREX
        'EURO BUND': 'FGBL @@-EUX', 
        'EURO BOBL': 'FGBM @@-EUX', 
        'EURO SCHATZ': 'FGBS @@-EUX', 
        'EURO BUXL': 'FGBX @@-EUX', 
        'EURO OAT LONG-TERM': 'FOAT @@-EUX', 
        'EURO BTP LONG-TERM': 'FBTP @@-EUX', 
        'EURO BTP SHORT-TERM': 'FBTS @@-EUX' 
    }

    symbols = _actives['smb_col']
    contracts = _actives['Contract']
    
    A = [x.split(' ')[0][-1] + x.split(' ')[0][-3] for (x, y) in zip(contracts, symbols) if dict[y].endswith('-ICE')] #else [x.split(' ')[0][-3] + x.split(' ')[0][-1] for x in contracts]
    B = [x.split(' ')[0][-3] + x.split(' ')[0][-1] for (x, y) in zip(contracts, symbols) if not dict[y].endswith('-ICE')]
    contracts = A + B    
    _actives['es_smb'] = [dict[x].replace('@@', y) if dict.get(x) != None else None for x, y in zip(symbols, contracts)]    
    #_actives = _actives[pd.notnull(_actives['es_smb'])]
    _actives = _actives[pd.notnull(_actives['es_smb'])]
    return _actives['es_smb']

def barchart_es_symbol_mapping(_actives):

    print('barchart to es symbol mapping')

    dict = {
        # SHFE - to add IMCI A0-SHF Nonferrous Metals Index
        'SHFE ALUMINIUM': 'AL @@-SHF', 
        'SHFE COPPER': 'CU @@-SHF',         
        'SHFE ZINC': 'ZN @@-SHF', 
        'SHFE GOLD': 'AU @@-SHF', 
        'SHFE SILVER': 'AG @@-SHF', 
        'SHFE RUBBER': 'RU @@-SHF', 
        'SHFE STEEL REBAR': 'RB @@-SHF', 
        'SHFE LEAD': 'PB @@-SHF', 
        'SHFE NICKEL': 'NI @@-SHF', 
        'SHFE TIN': 'SN @@-SHF', 
        'SHFE BITUMEN': 'BU @@-SHF', 
        'SHFE HOT-ROLLED COIL': 'HC @@-SHF',
        # DGCX
        'DGCX INDIAN RUPEE': 'DINR @@-DGC',
        # HKFE to add DHH - HSCEI Dividend Point, CUS - RENMENBi CURRENCY
        'HANG SENG INDEX': 'HSI @@-HKF', 
        'MINI HANG SENG INDEX': 'MHI @@-HKF', 
        'H-SHARES INDEX': 'HHI @@-HKF', 
        'MINI H-SHARES INDEX': 'MCH @@-HKF',   
        # LME
        # '': 'CAD M3-LME', '': 'NID M3-LME', '': 'ZSD M3-LME', '': 'AHD M3-LME', '': 'PBD M3-LME', 
        # MDEX 
        # '': '', '': '', '': '', '': '', '': '', 
        # TFEX - to add Individual Stock Futures
        'THAI SET50 INDEX': 'S50 @@-TFE', 
        # TAIWA
        # 'TAIWAN WEIGHTED IDX': '', <- couldn't find on search tool
        # KFE
        # '': '', '': '', '': '',
        # DME
        'DME CRUDE OIL OMAN': 'OQ @@-DME', 
        # DCE
        'DCE NO.2 SOYBEANS': 'B @@-DCE', 
        'DCE NO.1 SOYBEANS': 'A @@-DCE', 
        'DCE SOY MEAL': 'M @@-DCE', 
        'DCE CORN': 'C @@-DCE', 
        'DCE CORN STARCH': 'CS @@-DCE', 
        'DCE SOYBEAN OIL': 'Y @@-DCE', 
        'DCE POLYETHYLENE': 'L @@-DCE', 
        'DCE POLYPROPYLENE': 'PP @@-DCE', 
        'DCE PALM OLEIN': 'P @@-DCE', 
        'DCE PVC': 'V @@-DCE', 
        'DCE COKE': 'J @@-DCE', 
        'DCE COKING COAL': 'JM @@-DCE', 
        'DCE IRON ORE': 'I @@-DCE', 
        'DCE FRESH HEN EGG': 'JD @@-DCE', 
        'DCE FIBERBOARD': 'FB @@-DCE', 
        'DCE BLOCKBOARD': 'BB @@-DCE',
        # CZCE
        'CZCE PURE TERE ACID': 'TA @@-ZCE', 
        'CZCE SUGAR WHITE': 'SR @@-ZCE', 
        'CZCE COTTON NO.1': 'CF @@-ZCE', 
        'CZCE METHANOL': 'MA @@-ZCE',         
        'CZCE RAPESEED OIL': 'OI @@-ZCE',         
        'CZCE FLAT GLASS': 'FG @@-ZCE',             
        'CZCE RAPESEED MEAL': 'RM @@-ZCE', 
        'CZCE THERMAL COAL100': 'ZC @@-ZCE', 
        # NCDEX
        'NCDEX CHANA': 'CHARJDDEL @@-NCD', 
        'NCDEX SOYBEANS': 'SYBEANIDR @@-NCD',
        'NCDEX SOYBEAN OIL': 'SYOREF @@-NCD', 
        #'NCDEX RAPE MUSTARD': '', ?
        'NCDEX CORIANDER': 'DHANIYA @@-NCD', 
        'NCDEX JEERA': 'JEERAUNJHA @@-NCD',         
        'NCDEX TURMERIC': 'TMCFGRNZM @@-NCD',        
        'NCDEX KAPAS V 797': 'KAPASSRNR @@-NCD',         
        'NCDEX GUARSEED': 'GUARSEED10 @@-NCD', 
        'NCDEX GUARGUM': 'GUARGUM @@-NCD',
        # OSAKA
        'NIKKEI 225': 'NI225 @@-OSM', 
        'NIKKEI 225 MINI': 'NI225_M @@-OSM', 
        'NIKKEI 400': 'JN400 @@-OSM', 
        'TOKYO TOPIX INDEX': 'TOPIX @@-OSM', 
        'TOKYO TOPIX-MINI': 'TOPIXM @@-OSM', 
        'JPX 10-YEAR JGB': 'JGBL @@-JGB',         
        # NSE
        'NSE CNX NIFTY': 'NIFTY @@-NSF', 
        'NSE CNX IT INDEX': 'CNXIT @@-NSF', 
        'NSE BANK NIFTY INDEX': 'BANKNIFTY @@-NSF', 
        # CFE
        'S&P 500 VIX': 'VX @@-CF', 
        # MGEX
        'SPRING WHEAT': 'QMW @@', 
        # MNTRL        
        'CANADIAN 10-YEAR': 'CGB @@-MX',       
        'CANADIAN BA': 'BAX @@-MX', 
        'S&P TSX 60': 'SXF @@-MX',  
        # OMX
        'OMX SWEDISH INDEX': 'OMXS30 @@-OMF', 
        # BMF
        '1-DAY INTERBANK DEP.': 'DI1 @@-BMF', 
        'BMF LIVE CATTLE': 'BGI @@-BMF', 
        'BMF US DOLLAR': 'DOL @@-BMF', 
        'BMF BOVESPA INDEX': 'IND @@-BMF', 
        'BMF BOVESPA MINI': 'WIN @@-BMF', 
        # MATBA
        #'TRIGO B.A.': '', 
        #'MAIZ ROS': '', 
        #'SOJA ROS': '', 
        # TOCOM
        'TOCOM CRUDE OIL': 'TCL @@-TCM', 
        'TOCOM GOLD': 'TGD @@-TCM', 
        'TOCOM PLATINUM': 'TPL @@-TCM', 
        'TOCOM GASOLINE': 'THU @@-TCM', 
        'TOCOM KEROSENE': 'THO @@-TCM', 
        'TOCOM RUBBER': 'TRB @@-TCM', 
        'TOCOM PALLADIUM': 'TPA @@-TCM', 
        'TOCOM SILVER': 'TSI @@-TCM', 
        'TOCOM CORN': 'CORN @@-TGE', 
        'TOCOM U.S. SOYBEANS': 'SOYB @@-TGE',
        # SAFEX
        'JSE FTSE TOP 40': 'ALSI @@-SAF', 
        # '': 'ALMI @@-SAF'
        'JSE FTSE WEIGHTED 40': 'DTOP @@-SAF', 
        'SAFEX WHITE MAIZE': 'WMAZ @@-SAF', 
        # ICE-USA
        'COFFEE': 'KC @@', 
        'COCOA': 'CC @@', 
        'SUGAR #11': 'SB @@', 
        'COTTON #2': 'CT @@', 
        # ICE-EU
        'COCOA #7': 'C @@-ICE', 
        'ROBUSTA COFFEE 10-T': 'RC @@-ICE', 
        'WHITE SUGAR #5': 'W @@-ICE', 
        # ICE-IND : to add MFS, MMW, MWL, MFSB, MMWB
        'RUSSELL 2000 MINI': 'TF @@', 
        'MSCI EMI INDEX': 'MME @@',         
        # ICE
        'CRUDE OIL BRENT': 'BRN @@-ICE', 
        'CRUDE OIL WTI ICE': 'WBS @@-ICE',         
        'ICE GAS OIL LS': 'GAS @@-ICE', 
        'ICE NATURAL GAS': 'GWM @@-ICE',
        'ICE RBOB BLENDSTOCK': 'UHU @@-ICE', 
        'ICE HEATING OIL': 'UHO @@-ICE', 
        'ICE EUA FUTURES': 'ECF @@-ICE',
        # LIF-IND
        'FTSE 100': 'Z @@-ICL', 
        # LIFFFE
        '3-MONTH EURIBOR': 'I @@-ICL', 
        '3-MONTH STERLING': 'L @@-ICL', 
        '3-MONTH EUROSWISS': 'S @@-ICL', 
        # WPG
        'CANOLA': 'RS @@-WC', 
        # COMEX
        'HIGH GRADE COPPER': 'HG @@', 
        'GOLD': 'GC @@', 
        'SILVER': 'SI @@', 
        # NYMEX
        'CRUDE OIL WTI': 'CL @@', 
        'ULSD NY HARBOR': 'HO @@', 
        'GASOLINE RBOB': 'XRB @@', 
        'NATURAL GAS': 'NG @@', 
        'PLATINUM': 'PL @@', 
        'PALLADIUM': 'PS @@', 
        # CME
        'FEEDER CATTLE': 'FC @@', 
        'LIVE CATTLE': 'LC @@', 
        'LEAN HOGS': 'HE @@', 
        # IMM-CUR
        'AUSTRALIAN DOLLAR': '6A @@', 
        'BRITISH POUND': '6B @@', 
        'CANADIAN DOLLAR': '6C @@', 
        'JAPANESE YEN': '6J @@',
        'EURO FX': '6E @@',
        'SWISS FRANC': '6S @@', 
        'MEXICAN PESO': '6M @@', 
        'BRAZILIAN REAL': '6L @@', 
        'NEW ZEALAND DOLLAR': '6N @@', 
        'RUSSIAN RUBLE': '6R @@', 
        'SOUTH AFRICAN RAND': '6Z @@',
        'EURO/POUND': 'RP @@', 
        'EURO/YEN': 'RY @@', 
        'EURO/SWISS': 'RF @@',         
        # IOM
        'S&P 500 INDEX': 'SP @@', 
        'NIKKEI 225 YEN': 'NIY @@',
        # IMM-FIN
        'EURODOLLAR': 'GE @@',
        # GLBX
        'E-MINI S&P 500': 'ES @@', 
        'E-MINI S&P MIDCAP': 'MC @@', 
        'E-MINI NASDAQ 100': 'NQ @@', 
        'E-MINI JAPANESE YEN': 'ZJ @@', 
        'E-MINI EURO FX': 'ZE @@', 
        'E-MICRO GBP/USD': 'M6B @@', 
        'E-MICRO EUR/USD': 'M6E @@', 
        'E-MICRO AUD/USD': 'M6A @@',
        # CBOT-AGR
        'CORN': 'ZC @@', 
        'WHEAT': 'ZW @@', 
        'HARD RED WHEAT': 'KE @@', 
        'SOYBEANS': 'ZS @@', 
        'SOYBEAN MEAL': 'ZM @@', 
        'SOYBEAN OIL': 'ZL @@', 
        # CBOT-FIN
        'T-BOND': 'ZB @@', 
        '10-YEAR T-NOTE': 'ZN @@', 
        '5-YEAR T-NOTE': 'ZF @@', 
        '3-YEAR T-NOTE': 'ZE @@', 
        '2-YEAR T-NOTE': 'ZT @@', 
        'ULTRA T-BOND': 'UL @@', 
        'ULTRA 10-YEAR T-NOTE': 'TN @@', 
        '30-DAY FED FUNDS': 'ZQ @@',
        # CBOT-M
        'DJIA MINI-SIZED': 'YM @@',
        # EUR-IND
        'STOXX EUROPE 600': 'FXXP @@-EUX', 
        'EURO STOXX DIVIDEND': 'FEXD @@-EUX', 
        'VSTOXX MINI': 'FVS @@-EUX',         
        #'STOXX 50 ? ' : 'FESX @@-EUX', 
        #'DAX ? ' : 'FDAX @@-EUX', 
        # EUREX
        'EURO BUND': 'FGBL @@-EUX', 
        'EURO BOBL': 'FGBM @@-EUX', 
        'EURO SCHATZ': 'FGBS @@-EUX', 
        'EURO BUXL': 'FGBX @@-EUX', 
        'EURO OAT LONG-TERM': 'FOAT @@-EUX', 
        'EURO BTP LONG-TERM': 'FBTP @@-EUX', 
        'EURO BTP SHORT-TERM': 'FBTS @@-EUX' 
    }

    symbols = _actives['smb_col']
    contracts = _actives['Contract']
    
    #print([dict[y] for y in symbols])    
    A = [x.split(' ')[0][-1] + x.split(' ')[0][-3] for (x, y) in zip(contracts, symbols) if dict[y.upper()].endswith('-ICE')] #else [x.split(' ')[0][-3] + x.split(' ')[0][-1] for x in contracts]
    B = [x.split(' ')[0][-3] + x.split(' ')[0][-1] for (x, y) in zip(contracts, symbols) if not dict[y.upper()].endswith('-ICE')]
    contracts = A + B
    #print(contracts)    
    
    # ex. @@ ⇒ H7, G8
    _actives['es_smb'] = [dict[x.upper()].replace('@@', y) if dict.get(x.upper()) != None else None for x, y in zip(symbols, contracts)]    
    #_actives = _actives[pd.notnull(_actives['es_smb'])]
    _actives = _actives[pd.notnull(_actives['es_smb'])]
    return _actives['es_smb'].tolist()

def get_contract_of_FGHJKMNQUVXZ(_product): # VX, FCE
    yr =  datetime.datetime.now().year
    mth = datetime.datetime.now().month
    dict = {
        1:'F', 2:'G', 3:'H', 4:'J', 5:'K', 6:'M',
        7:'N', 8:'Q', 9:'U', 10:'V', 11:'X', 12:'Z',
    }
    first = dict[mth] + str(yr)[-1]
    second = dict[(mth + 1) % 12] + str(yr)[-1] if mth < 12 else dict[(mth + 1) % 12] + str(yr + 1)[-1]

    first = _product.replace('@@', first)
    second = _product.replace('@@', second)

    return [first, second]

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

    # look 7 days back in case some files missing
    for lookbackdays in [0]:

        from datetime import timedelta # http://stackoverflow.com/questions/6871016/adding-5-days-to-a-date-in-python
        today = dpu.date_to_str(datetime.date.today() + timedelta(days=lookbackdays)) #'2016-05-28'

        # get daily active symbols for TOCOM
        save_file = 'G:/中心限月/tocom/{0}.csv'.format(today)
        year = today.split('-')[0];
        month = today.split('-')[1].split('-')[0];
        day = today.split('-')[-1]
        if not dtu.is_weekday(year, month, day):
            continue
        GetTocomActiveMonth(save_file, int(year), int(month))

        # get daily active symbols for all other exchanges    
        get_and_save_futures_market_symbol_list(today) #if not iou.file_exist('barchart_smb_{0}.xlsx'.format(today)) else None   
        targets = [ 'mgex', 'shfe', 'dgcx', 'hkfe', 'tfex', 'taiwa', 'dme', 'dce', 'czce', 'ncdex', 'osaka', #'nse', 
                        'cfe', 'mntrl','omx', 'bmf', 'safex', #'matba',
                        'ice-us-commodities', 'ice-us-indices', 'ice-us-currencies',
                        'ice-eu-commodities', 'ice-eu-indices', 'ice-eu-energies', 'ice-eu-interest-rates',
                        'ice-ca', 'comex-metals', 
                        'nymex-energies', #'nymex-softs', 
                        'cme-ag','cme-currencies', 'cme-indices', 'cme-interest-rates', 'cme-mini', # 'cme-housing',
                        'cbot-ag', 'cbot-interest-rates', # , 'cbot-indices'
                        'eurex-indices', 'eurex-interest-rates', 
                        #'tocom', <- tocom has its own active month engine
                        # 'lme', 'mdex','kfe', 
                        ]
        barchart_actives_list = [find_active_month_from_symbol_list_table('G:/中心限月/Barchart/barchart_{0}_{1}.xlsx'.format(x, today), x) for x in targets]
        #pd.DataFrame(barchart_actives_list).to_csv('G:/中心限月/Barchart/barchart_actives_list_{0}.csv'.format(today))

        # mapping to es symbols
        es_actives_list = []
        for x in barchart_actives_list:
            es_actives_list.extend(barchart_es_symbol_mapping(x))

        # actives not-in-barchart page
        es_actives_list.extend(get_contract_continuous_month('DX @@'))
        es_actives_list.extend(get_contract_of_HMUZ('YM @@'))
        es_actives_list.extend(get_contract_of_HMUZ('FDAX @@-EUX'))
        es_actives_list.extend(get_contract_of_HMUZ('ES @@'))
        es_actives_list.extend(get_contract_of_FGHJKMNQUVXZ('VX @@-CF'))
        es_actives_list.extend(get_contract_of_FGHJKMNQUVXZ('FCE @@-EEI'))

        es_actives_list.extend(get_open_interest_symbol())
        # save tickers to a csv

        pd.DataFrame(es_actives_list).to_csv('G:/中心限月/eSignal/es_actives_list_{0}.csv'.format(today),index=False,header=False)

        # get es symbol without contract month
        #es_actives_unique_symbol_list = [x.split(' ')[0] for x in es_actives_list if not isinstance(x, list)]
        #es_actives_unique_exchange_postfix_list = [x.split('-').pop() for x in es_actives_list if not isinstance(x, list)]
        #es_actives_unique_list = []
        #for product, exchange in zip(es_actives_unique_symbol_list, es_actives_unique_exchange_postfix_list):
        #    es_actives_unique_list.extend(product + ' @@-' + exchange)

        ## get unique
        #es_actives_unique_list = list(set(es_actives_unique_list))

        #es_oi_list = [get_contract_oi(x) for x in es_actives_unique_list]    # open intere est

        # !-- obsolete --!
        ## save tickers to a csv
        #outfile = 'final_es_actives_{0}.csv'.format(today)
        #iou.delete_file(outfile)
        #for l in es_actives_list:
        #    print(l)
        #    iou.write_csv(l, outfile, _isAppend=True, _isHeader=False)

        ## actives' OI ticker
        #for l in get_open_interest_symbol():
        #    print(l)
        #    iou.write_csv(l, outfile, _isAppend=True, _isHeader=False)

        # !-- to be implemented --!
        # mapping to tt symbols
        #tt_actives_list = [barchart_tt_symbol_mapping(x) for x in barchart_actives_list]
        #outfile = 'tt_actives_{0}.csv'.format(today)
        #iou.delete_file(outfile)
        #for l in es_actives_list:
        #    print(l)
        #    iou.write_csv(l, outfile, _isAppend=True, _isHeader=False)
