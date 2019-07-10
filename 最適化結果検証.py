import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\PyOptimization".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')
sys.path.append('../PyHadoop/')

#from bs4 import BeautifulSoup

import OsUtility as osu
import IoUtility as iou
import WebUtility as wbu
import ZipUtility as zpu
import DatetimeUtility as dtu
import DataTypeUtility as dpu
import DataCollectionUtility as dcu
import PyUbuntu as ubu
import ZipUtility as zpu
import FontUtility as ftu
import PyUbuntu as ubu

import time
import datetime
import random

from numba import jit
import numpy as np
import queue
import threading

import array, json
import matplotlib.pyplot as plt

import datetime as dt

import DataTypeUtility as dpu
import DatetimeUtility as dtu

from deap import algorithms
from deap import base
from deap import creator
from deap import tools

def verify_simulation_and_prepare_viz_data_for_tableau(_login_user, _remote_host, _sim_ticker_and_tick_size_open_close_time_str, _exchange, _sim_start_date_str, _sim_strategy_name_and_param):

    #login_user = 'p000505'
    #target_remote_host = ''
    #sim_ticker_and_tick_size_open_close_time_str = ''
    #exchange = 'TSE'
    #sim_start_date_str = '2017-05-09'
    #sim_strategy_name_and_param = "mp_stat_strategy={0}={1}={2}={3}={4}={5}={6}={7}={8}={9}={10}".format(individual[0], individual[1], individual[2], individual[3], individual[4], individual[5], individual[6], individual[7], individual[8], individual[9], individual[10]);
    #param_files = iou.getfiles('G:/QOptimizationResult/{0}/{1}/'.format(exchange, ticker), '.csv', 'best_parameter_mp_stat_strategy')

    # Step.0 Run the CppKimG++.cpp simulation with the best parameters we've found so far so that we get the daily trade results
    import subprocess
    remote_linux_non_shell_cmd = 'plink -pw M9rgan?? {0}@{1} "/home/{0}/projects/CppKimG++_debugprint.out" --ticker_tick_open_close {2} --exchange {3} --date_of_start {4} --strategy_name_and_param {5};exit;'.format(_login_user, _remote_host, _sim_ticker_and_tick_size_open_close_time_str, _exchange, _sim_start_date_str, _sim_strategy_name_and_param)
    print(remote_linux_non_shell_cmd + '\n')
    proc, err = subprocess.Popen(remote_linux_non_shell_cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()   # stdout : http://docs.python.jp/3/library/subprocess.html
    err_msg = err.decode('utf-8')
    #[print(line.replace('\n','')) for line in proc.stdout.decode('utf-8')]
    [print(line.replace('\n','')) for line in proc.decode('utf-8').splitlines()]        

    ticker = _sim_ticker_and_tick_size_open_close_time_str.split('=')[0]
    today = dt.date.today() #.strftime('%Y-%m-%d')
    sim_date = dpu.str_to_date(_sim_start_date_str)
    folders = iou.getfolders('G:/QLevel1Data/{0}/'.format(_exchange))
    folders = [f for f in folders if f >= _sim_start_date_str]
    #while sim_date < today:
    for folder in folders:
        
        #if not dtu.is_weekday(today.year, today.month, today.day):
        #    continue

        # Step.1 Linux����������t�@�C�� Kim/TRADE_LOG/TSE/2017-08-01/....csv �����Ă���B-> G:/QTrace/TSE/2017-08-01/
        linux_from_remote_dir = '/home/{0}/Kim/TRADE_LOG/{1}/{2}/{3}_D_{2}.csv'.format(_login_user, _exchange, sim_date, ticker)
        win_to_local_folder = 'G:/QTrace/{0}/TRADE_LOG/'.format(_exchange) #, ticker)
        osu.create_folder_if_not_exist(win_to_local_folder)
        ubu.file_transfer_linux_remote_to_win_local(_remote_host, linux_from_remote_dir, win_to_local_folder)

        # Step.2 Linux�����璍���ύX���t�@�C�� Kim/STRATEGY_LOG/TSE/3436-TSE/2017-10-03/....csv �����Ă��� -> G:/QTrace/TSE/2017-08-01/
        linux_from_remote_dir = '/home/{0}/Kim/STRATEGY_LOG/{1}/{2}/{3}_D_{2}.csv'.format(_login_user, _exchange, sim_date, ticker)
        win_to_local_folder = 'G:/QTrace/{0}/STRATEGY_LOG/'.format(_exchange) #, ticker)
        osu.create_folder_if_not_exist(win_to_local_folder)
        ubu.file_transfer_linux_remote_to_win_local(_remote_host, linux_from_remote_dir, win_to_local_folder)

        # Step.3 Windows�� G:/QSimulationData/TSE/3436-TSE/2017-08-01/....gz ��𓀂��� -> G:/QTrace/TSE/2017-08-01/
        gzip_file = 'G:\QLevel1Data\{0}\{1}\{2}_0_{1}_utf8.csv.gz'.format(_exchange, sim_date, ticker)
        copied_gzip_file = 'G:\QTrace\{0}\Level1\{1}\{2}_0_{1}_utf8.csv.gz'.format(_exchange, sim_date, ticker)
        to_folder = 'G:\QTrace\{0}\Level1\{1}\\'.format(_exchange, sim_date)
        osu.create_folder_if_not_exist(to_folder)
        local_windows_non_shell_cmd = 'copy /Y ' + gzip_file + ' ' + to_folder + ' & "D:/Program Files/7-Zip/7z.exe" x ' + gzip_file + ' -aos -o' + to_folder + ' & del ' + copied_gzip_file  #+ ';'
        print(local_windows_non_shell_cmd + '\n')
        proc = subprocess.Popen(local_windows_non_shell_cmd, shell=True, stdout=subprocess.PIPE)   # stdout : http://docs.python.jp/3/library/subprocess.html

        sim_date = dtu.date_add(sim_date, 1)
    
    print()
