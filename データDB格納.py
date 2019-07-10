# -*- coding:shift-jis -*-
import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../x64/Release/')
sys.path.append('../PyUtility/')

import IoUtility as iou
import DbUtility as dbu
import DatetimeUtility as dtu

import time
import datetime as dt
from subprocess import call

import pandas as pd
import zipfile

import re

DSN = "STEVE-PC"
Database = "eSignal_201606"
UserId = "sa"
Password = "Bigdata01"
Table = "FUTURES"
odbcDSN = "eSignal"
odbcUserId = "sa"
odbcPassword = "Bigdata01"
CsvFolder = 'G:/QCollector Expert For eSignal/FUTURES/'

#DSN = 'P11013'
#Database = 'DWConfiguration'
#UserId = 'sa'
#Password = 'Bigdata01'
#Table = 'tick'
#odbcDSN = 'P11013-D'
#odbcUserId = 'sa'
#odbcPassword = 'Bigdata01'
#CsvFolder = './ticks'

if __name__ == "__main__":
    #dbu.bulkcopy_es_tick(DSN, Database, UserId, Password, Table, odbcDSN, odbcUserId, odbcPassword, CsvFolder)
    #dbu.bulkcopy_es_daily(DSN, Database, UserId, Password, Table, odbcDSN, odbcUserId, odbcPassword, CsvFolder)
    #dbu.bulkcopy_es_smmry(DSN, Database, UserId, Password, Table, odbcDSN, odbcUserId, odbcPassword, CsvFolder)
    
    # 1. Insert and update daily ticks into [eSignal].[dbo].[FUTURES]
    # http://stackoverflow.com/questions/3781851/run-a-python-script-from-another-python-script-passing-in-args
    data_folder = "G:/QCollector Expert For eSignal/TSE/"
    #db_table = "TSE"
    #os.system("python C:/Users/steve/Desktop/PySong2/PythonUtility/DbUtility.py \"{0}\" \"{1}\"".format(data_folder,db_table))   
    # 2. Redistribute the data in [eSignal].[dbo].[FUTURES] into corresponding monthly data tables such as [eSignal_201608].[dbo].[FUTURES], [eSignal_201609].[dbo].[FUTURES] and so on...
    # http://stackoverflow.com/questions/34296845/python-pyodbc-execute-stored-procedure-with-parameters   
    year = 2016
    month = 9
    symbol = '6%'
    sql = "exec eSignal.dbo.RedistributeTicksData {0}, {1}, '{2}'".format(year, month, symbol)
    dbu.mssql_exec(DSN, UserId, Password, Database, sql)
    # 3. Delete duplicates record from each monthly data tables
    # http://stackoverflow.com/questions/26940127/sql-query-fails-when-using-pyodbc-but-works-in-sql?answertab=votes#tab-top
    #Database = 'eSignal_201609'
    #Table = 'TEST2'
    #sql = """WITH CTE AS(
    #           SELECT *,
    #               RN = ROW_NUMBER()OVER(PARTITION BY Datetime,Symbol,AutoId ORDER BY Datetime,Symbol,AutoId)
    #           FROM [{0}].[dbo].[{1}]
    #        )
    #        DELETE FROM CTE WHERE RN > 1""".format(Database,Table)
    #dbu.mssql_exec(DSN, UserId, Password, Database, sql)
    # 4. Compare data in each monthly data table with the original csv. If everything is ok, delete the pooled data from the [eSignal].[dbo].[FUTURES]
    #for file in iou.getfiles(data_folder,'.zip'):
    #    # read in csv as dataframe from zipped archive
    #    z = zipfile.ZipFile(file,'r')
    #    df = pd.read_csv(z.open(file.replace('.zip','.csv')))
    #    rows = len(df)
    #    # http://stackoverflow.com/questions/18885175/read-a-zipped-file-as-a-pandas-dataframe
    #    sDate = file.split('_0_')[1].replace('.zip','')
    #    eDate = dtu.dateadd(sDate, 1)
    #    sql = """SELECT COUNT(*) FROM [{0}].[dbo].[{1}] WHERE Symbol={2} and Datetime >= {3} 07:00:00 and Datetime < {4} 07:00:00""".format(Database,Table,symbol,sDate,eDate)
    #    res = dbu.mssql_exec(DSN, UserId, Password, Database, sql)
    #    # 
    #    if rows != res.fetch_one():        
    #        print('record data mismatch @',file)
    print()
