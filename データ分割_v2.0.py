# -*- coding:shift-jis -*-

import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')
sys.path.append('../PyHadoop/')

import ctypes

import datetime as dt
import quandl as ql
import pandas as pd
import numpy
import csv

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

import time
import threading
import queue
import subprocess

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

def divide_to_day2day_old(_folder, _files, _open, _close): # old design used for Windows ⇒ inaccurate and replaced totally with Linux logic with simple 3 shell commands
    
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
            day_df = df[(df['datetime'] >= dpu.str_to_datetime('{0} {1}'.format(target_open_date,_open))) & (df['datetime'] <= dpu.str_to_datetime('{0} {1}'.format(target_close_date,_close)))]        
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
            remaining_df = df[df['datetime'] >= dpu.str_to_datetime('{0} {1}'.format(next_open_date,_open))]
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

def check_if_any_remote_machine_already_has_the_csv_data(_remote_host_list, _linux_user = 'p000505', _exchange='', _synchronised_queue=queue.Queue()):

    # ======== 転送予定ファイルを余計に転送しない工夫 ========
    #osu.set_console_title('転送予定ファイルを既に保持するマシンが有るか確認 ' + dtu.get_currnet_datetime_str())
    ftu.print_stdout_with_color('======== 転送予定ファイルを既に保持するマシンが有るか確認 ========\n', 'blue', 'yellow')
    tick_folder = 'G:/QCollector_Expert_For_eSignal/{0}/'.format(_exchange)
    win_local_csv_files = iou.getfiles(tick_folder,'_0.csv')

    machine_with_overlap_files = {}
    machine_with_unprocessed_csv_files = {}
    machine_with_unprocessed_gz_files = {}
    machine_with_ungzed_csv_files = {}

    minimum_overlap_threshold = 50 # with more than 50 overlaping files, we consider using this remote machine

    for remote_host in _remote_host_list:
        linux_shell = 'plink -pw M9rgan?? {0}@{1} ls "/home/{0}/QCollector_Expert_For_eSignal/{2}/*.csv"'.format(_linux_user, remote_host, _exchange)        
        #linux_shell = 'plink -pw M9rgan?? {0}@{1} find /home/{0}/QCollector_Expert_For_eSignal/{2}/ -name \\\"*.csv\\\" -type f -print | wc -l'.format(_linux_user, remote_host, _exchange)
        print(linux_shell)
        result, err = subprocess.Popen(linux_shell,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()    # stdout : http://docs.python.jp/3/library/subprocess.html
        err_msg = err.decode('utf-8') # https://stackoverflow.com/questions/33155903/popen-object-not-iterable
        try:
            remaining_csv_files_on_linux = [line.replace('\n','').split(exchange+'/')[1] for line in result.decode('utf-8').splitlines()]
            overlap_file_num = len(set(remaining_csv_files_on_linux).intersection(win_local_csv_files))
        except:
            now = dtu.get_currnet_datetime_str()
            this_file, line_no = osu.get_current_line_number_and_file_name()
            print("{0}, {1}, {2}, {3}, Exception decode('utf-8') Poprn result of raw csv files".format(now, this_file, line_no, _exchange))
            remaining_csv_files_on_linux = [] # dummy
            overlap_file_num = 0              # dummy <- can be any number
        # 重複ファイルの数
        machine_with_overlap_files[remote_host] = overlap_file_num
        # Linux側未処理.csv ファイル
        remaining_csv_file_num = 9999999999 if 'list too long' in err_msg else len(remaining_csv_files_on_linux)
        machine_with_unprocessed_csv_files[remote_host] = remaining_csv_file_num
        # AnalysisData ディレクトリ内 .gz ファイル        
        linux_shell = 'plink -pw M9rgan?? {0}@{1} ls "/home/{0}/AnalysisData/{2}/*_utf8.csv.gz"'.format(_linux_user, remote_host, _exchange)
        #linux_shell = 'plink -pw M9rgan?? {0}@{1} find /home/{0}/AnalysisData/{2}/ -name \\\"*_utf8.csv.gz\\\" -type f -print | wc -l'.format(_linux_user, remote_host, _exchange)
        print(linux_shell)
        result,err = subprocess.Popen(linux_shell,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()    # stdout : http://docs.python.jp/3/library/subprocess.html
        err_msg = err.decode('utf-8') # https://stackoverflow.com/questions/33155903/popen-object-not-iterable
        try:
            remaining_gz_files_on_linux = [line.replace('\n','').split(exchange+'/')[1] for line in result.decode('utf-8').splitlines()]        
            remaining_gz_file_num = 9999999999 if 'list too long' in err_msg else len(remaining_gz_files_on_linux)
        except:
            now = dtu.get_currnet_datetime_str()
            this_file, line_no = osu.get_current_line_number_and_file_name()
            print("{0}, {1}, {2}, {3}, Exception decode('utf-8') Popen result of gz files".format(now, this_file, line_no, _exchange))
            remaining_gz_files_on_linux = [] # dummy
            remaining_gz_file_num = 0        # dummy <- can be any number
        machine_with_unprocessed_gz_files [remote_host] = remaining_gz_file_num
        # AnalysisData ディレクトリ内 .csv ファイル        
        linux_shell = 'plink -pw M9rgan?? {0}@{1} ls "/home/{0}/AnalysisData/{2}/*.csv"'.format(_linux_user, remote_host, _exchange)
        #linux_shell = 'plink -pw M9rgan?? {0}@{1} find /home/{0}/AnalysisData/{2}/ -name \\\"*.csv\\\" -type f -print | wc -l'.format(_linux_user, remote_host, _exchange)
        print(linux_shell)
        result,err = subprocess.Popen(linux_shell,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()    # stdout : http://docs.python.jp/3/library/subprocess.html
        err_msg = err.decode('utf-8') # https://stackoverflow.com/questions/33155903/popen-object-not-iterable
        try:
            ungzed_csv_files_on_linux = [line.replace('\n','').split(exchange+'/')[1] for line in result.decode('utf-8').splitlines()]        
            ungzed_csv_file_num = 9999999999 if 'list too long' in err_msg else len(ungzed_csv_files_on_linux)
        except:
            now = dtu.get_currnet_datetime_str()
            this_file, line_no = osu.get_current_line_number_and_file_name()
            print("{0}, {1}, {2}, {3}, Exception decode('utf-8') Popen result of transformed csv files".format(now, this_file, line_no, _exchange))
            ungzed_csv_files_on_linux = []  # dummy
            ungzed_csv_file_num = 0         # dummy <- can be any number
        machine_with_ungzed_csv_files [remote_host] = ungzed_csv_file_num

    # select the remote machine with the most overlapping files stored or select the machine with the most unprocessed files in it.
    # dicttionary's key with max value : https://stackoverflow.com/questions/268272/getting-key-with-maximum-value-in-dictionary
    #                                    https://stackoverflow.com/questions/30418481/error-dict-object-has-no-attribute-iteritems-when-trying-to-use-networkx
    import operator
    remote_machine_with_max_overlapping_files = max(machine_with_overlap_files.items(), key=operator.itemgetter(1))[0]
    remote_machine_with_max_unprocessed_csv_files = max(machine_with_unprocessed_csv_files.items(), key=operator.itemgetter(1))[0]
    remote_machine_with_max_unprocessed_gz_files = max(machine_with_unprocessed_gz_files.items(), key=operator.itemgetter(1))[0]
    remote_machine_with_max_ungzed_csv_files = max(machine_with_ungzed_csv_files.items(), key=operator.itemgetter(1))[0]
    if machine_with_overlap_files[remote_machine_with_max_overlapping_files] > minimum_overlap_threshold:
        remote_priority_machine = remote_machine_with_max_overlapping_files
    elif machine_with_unprocessed_csv_files[remote_machine_with_max_unprocessed_csv_files] > 0:
        remote_priority_machine = remote_machine_with_max_unprocessed_csv_files
    elif machine_with_unprocessed_gz_files[remote_machine_with_max_unprocessed_gz_files] > 0:
        remote_priority_machine = remote_machine_with_max_unprocessed_gz_files
    elif machine_with_ungzed_csv_files[remote_machine_with_max_ungzed_csv_files] > 0:
        remote_priority_machine = remote_machine_with_max_ungzed_csv_files
    else:
        remote_priority_machine = None
    # 
    import collections # sort dictionary by key : https://stackoverflow.com/questions/9001509/how-can-i-sort-a-dictionary-by-key
    print("PC with overlapping csv : ",collections.OrderedDict(sorted(machine_with_overlap_files.items())))
    print("PC with remaining csv   : ",collections.OrderedDict(sorted(machine_with_unprocessed_csv_files.items())))
    print("PC with remaining gz    : ",collections.OrderedDict(sorted(machine_with_unprocessed_gz_files .items())))
    print("PC with transformed csv : ",collections.OrderedDict(sorted(machine_with_ungzed_csv_files .items())))
    ftu.print_stdout_with_color('Priority remote machine is {0}\n'.format(remote_priority_machine), 'blue', 'yellow')
    # user input prompt
    osu.input_with_timeout("Confirm the remote host selected...")
    _synchronised_queue.put(remote_priority_machine)
    return remote_priority_machine

#def determine_most_resourceful_remote_machine(_ambari_server_host, _remote_host_list, _linux_user = 'p000505', _synchronised_queue=queue.Queue()):

#    # ======== 計算処理割り当て先マシンを選別 ========
##    osu.set_console_title('計算処理割り当て先マシンを選別 ' + dtu.get_currnet_datetime_str())
#    ftu.print_stdout_with_color('======== 計算処理割り当て先マシンを選別  ========\n', 'blue', 'yellow')
##    ambari_server_host = '192.168.0.5'
##    remote_host_list = ['192.168.0.3', '192.168.0.5', '192.168.0.7',]

#    # find the remote host with the most idle CPU resource
#    #if len(param)==4:
#    if len(_remote_host_list)==1:
#        available_remote_host = _remote_host_list[0] # if there is only one candidate host in the list
#    else:
#        available_remote_host = osu.determine_resourceful_remote_linux(_remote_host_list, _linux_user)
#    _synchronised_queue.put(available_remote_host) # multi-threaded function call put value to queue
  
def reorganize_files_in_the_zipped_folder(_folder): # Windows side
        
    # ======== reorganize windows side zipped/ folder ========
#    osu.set_console_title('zippedフォルダ内既存ファイルを日付毎に整理 ' + dtu.get_currnet_datetime_str())
    ftu.print_stdout_with_color('======== zippedフォルダ内既存ファイルを日付毎に整理  ========\n', 'blue', 'yellow')
    for zip_file in iou.getfiles(_folder,'.zip'):
        # 日付部分抽出
        try:
            iso_dt = zip_file.split('_0_')[1].replace('.zip','')
        except:
            print('exception @ moving zip ... ',_folder + zip_file)
            continue
        # ファイル移動
        org_dir = _folder
        dst_dir = _folder+'/'+iso_dt+'/'
        dst_dir = dst_dir.replace('//','/')
        osu.create_folder_if_not_exist(dst_dir)
        iou.move_file(org_dir + zip_file, dst_dir + zip_file, True)

def reorganize_files_in_the_gz_data_folder(_folder): # Windows side

#    osu.set_console_title('既存解析用データフォルダ日付毎に整理 ' + dtu.get_currnet_datetime_str())
    ftu.print_stdout_with_color('======== 既存解析用データフォルダ日付毎に整理  ========\n', 'blue', 'yellow')
    #folder = 'G:/QLevel1Data/{0}/'.format(_exchange)
    for gz_file in iou.getfiles(_folder,'.gz'):
        # 日付部分抽出
        try:
            iso_dt = gz_file.split('_0_')[1].replace('_utf8.csv.gz','')
        except:
            print('exception @ moving gz ... ',_folder + gz_file)
            continue
        # ファイル移動
        org_dir = _folder
        dst_dir = _folder+'/'+iso_dt+'/'
        dst_dir = dst_dir.replace('//','/')
        osu.create_folder_if_not_exist(dst_dir)
        iou.move_file(org_dir + gz_file, dst_dir + gz_file, True)

def add_suffix_to_0_csv_file(_folder):

    # ======== 1. 名前重複で上書きされないように _0.csv ファイル名に日付を付加 ========    
    ftu.print_stdout_with_color('======== 名前重複で上書きされないように _0.csv ファイル名に日付を付加  ========\n', 'blue', 'yellow')
    for csv_file in iou.getfiles(_folder,'_0.csv'):
        csv_file = (_folder + '/' + csv_file).replace('//','/').replace('\\','/')
        last_update = osu.get_file_lastupdate(csv_file)
        if not csv_file.count('@') >= 2: # exclude files already has datetime in its name. ex.@2017-07-30 172320@
            osu.rename_file(csv_file, csv_file.replace('_0.csv','_@'+dpu.datetime_to_str(last_update).replace(':','')[:-7]+'@_0.csv'))

def find_opening_closing_time_by_ticker(_ticker):

    if "-TSE" in _ticker:
        open = "9:00:00"
        close = "15:00:00"
    else:
        open = ""
        close = ""

    return open, close

def calc_individual_tick_size(_exchange):
    
    data_folder = "G:/QCollector Expert For eSignal/{0}/".format(_exchange)
    out_folder = "G:/QTickSize/"
    iou.create_folder_if_not_exist(out_folder)
    tick_save_file = out_folder + _exchange + '.csv'
    if iou.file_exist(tick_save_file) and iou.get_num_of_lines_in_file(tick_save_file) > 0:
        df = pd.read_csv(tick_save_file, header=None)    # error -> iou.read_csv_into_list(tick_save_file,':') if iou.file_exist(tick_save_file) else []
        existing_tick_size = '\r\n'.join(df.ix[:,0]).split('\r\n')  #１列目
    else:
        existing_tick_size = []
    csv_list = iou.getfiles(data_folder + data_folder, '.csv')
    save_str_list = []
    tuple_list = []
    for i,file in enumerate(csv_list):
        osu.print_progress_bar_on_consle(i, len(csv_list))
        if len(file.split('_0_')) >= 2:
            ticker_name = file.split('_0_')[0]
        elif len(file.split('_@')) >= 2:
            ticker_name = file.split('_@')[0]
        else:
            print('could not find ticker name with file ',file)
            continue
        if not ticker_name in [x.split(':')[0] for x in existing_tick_size]:   # 既存 tick size になければ
            file_size = osu.get_file_size(data_folder + file)
            lines = iou.read_csv_into_list(data_folder + file)

            bid_list = [float(x.split(',')[6]) if not x.decode('utf-8').split(',')[6]=="" else 0.0 for x in lines]
            ask_list = [float(x.split(',')[9]) if not x.decode('utf-8').split(',')[9]=="" else 0.0 for x in lines]
            price_list = list(set(bid_list + ask_list))
            from itertools import combinations
            tick_size = abs(min(list(set([abs(a -b) for a, b in combinations(price_list, 2)])))) # https://stackoverflow.com/questions/18906491/getting-difference-from-all-possible-pairs-from-a-list-python
            tick_size = float(str(tick_size)) # 0.44999999999 -> 0.45
            try:
                repeating_decimal9_position = str(tick_size).index('9999') # 0.44999999999 -> 0.45
                tick_size = round(tick_size, repeating_decimal9_position)
            except:
                None
            try:
                repeating_decimal0_position = str(tick_size).index('0000') # 0.5000000001 -> 0.5
                tick_size = round(tick_size, repeating_decimal0_position)
            except:
                None
            #iou.str_to_txt(tick_save_file, '{0}:{1}'.format(ticker_name,tick_size), True)
            #iou.write_csv('{0}:{1}'.format(ticker_name, tick_size), tick_save_file, True, 1, 1, False, None, False, None, ':')
            open, close = find_opening_closing_time_by_ticker(ticker_name)
            tuple_list.append(('{0}={1}={2}={3}'.format(ticker_name, tick_size, open, close), file_size))
            tuple_list.sort(key=lambda x: x[1], reverse=True) # sort on file size in descending order https://stackoverflow.com/questions/14466068/sort-a-list-of-tuples-by-second-value-reverse-true-and-then-by-key-reverse-fal
            save_str_list = [x[0] for x in tuple_list]

    if len(csv_list) < 100: # not all files of individual ticker were found
        data_folder = "G:/QCollector Expert For eSignal/{0}/zipped/".format(_exchange)
        zip_folders = iou.getfolders(data_folder)
        if len(zip_folders)<2:
            return 0
        zip_folders.reverse()
        zip_folder = zip_folders[1]              # フォルダを名前順に逆ソートし、2番新しい日付のフォルダを対象とする
        zip_list = iou.getfiles(data_folder + zip_folder, '.zip')
        for i, zip_file in enumerate(zip_list):
            osu.print_progress_bar_on_consle(i, len(zip_list))
            if len(zip_file.split('_0_')) >= 2:
                ticker_name = zip_file.split('_0_')[0]
            else:
                print('Could not find ticker name with file ',zip_file)
                continue
            if not ticker_name in [x.split(':')[0] for x in existing_tick_size]:   # 既存 tick size になければ
                nrows = 100000
                file_size = osu.get_file_size(data_folder + zip_folder + '/' + zip_file)
                try:
                    lines = iou.read_zip_file(data_folder + zip_folder + '/' +zip_file, zip_file.replace('.zip', '.csv'), nrows)
                except:
                    print('Could not find .csv file inside of .zip file ',zip_file)
                    continue
                bid_list = [float(x.decode('utf-8').split(',')[6]) if not x.decode('utf-8').split(',')[6]=="" else 0.0 for x in lines]
                ask_list = [float(x.decode('utf-8').split(',')[9]) if not x.decode('utf-8').split(',')[9]=="" else 0.0 for x in lines]
                price_list = list(set(bid_list + ask_list))
                if len(price_list) >= 2:
                    from itertools import combinations
                    tick_size = abs(min(list(set([abs(a -b) for a, b in combinations(price_list, 2)])))) # https://stackoverflow.com/questions/18906491/getting-difference-from-all-possible-pairs-from-a-list-python
                    tick_size = float(str(tick_size)) # 0.44999999999 -> 0.45
                    try:
                        repeating_decimal9_position = str(tick_size).index('9999') # 0.44999999999 -> 0.45
                        tick_size = round(tick_size, repeating_decimal9_position)
                    except:
                        None
                    try:
                        repeating_decimal0_position = str(tick_size).index('0000') # 0.5000000001 -> 0.5
                        tick_size = round(tick_size, repeating_decimal0_position)
                    except:
                        None
                    #iou.str_to_txt(tick_save_file, '{0}:{1}'.format(ticker_name,tick_size), True)
                    #iou.write_csv('{0}:{1}'.format(ticker_name, tick_size), tick_save_file, True, 1, 1, False, None, False, None, ':')
                    open, close = find_opening_closing_time_by_ticker(ticker_name)
                    tuple_list.append(('{0}={1}={2}={3}'.format(ticker_name, tick_size, open, close), file_size))
                    tuple_list.sort(key=lambda x: x[1], reverse=True) # sort on file size in descending order https://stackoverflow.com/questions/14466068/sort-a-list-of-tuples-by-second-value-reverse-true-and-then-by-key-reverse-fal
                    save_str_list = [x[0] for x in tuple_list]
    pd.DataFrame(save_str_list).to_csv(tick_save_file, mode='a', index=False, header=False) # mode='a' -> append https://stackoverflow.com/questions/17530542/how-to-add-pandas-data-to-an-existing-csv-file

# divide tick files with multiple days' tick into single day's tick
if __name__ == '__main__':
    
    param = sys.argv

    #win_file_path = 'G:/QLevel1Data/TSE/*_utf8.csv'
    #linux_destination_directory_path = '/home/p000505/QCollector_Expert_For_eSignal/TEST/'
    #ubu.file_transfer_win_local_to_linux_local(win_file_path, '192.168.0.5', linux_destination_directory_path)

    now = dt.datetime.now()
    today = dt.datetime.today().strftime('%Y-%m-%d')
    hour = now.hour
    minute = now.minute

    from inspect import currentframe, getframeinfo
    frameinfo = getframeinfo(currentframe())
    this_file_name = frameinfo.filename[frameinfo.filename.rfind('\\')+1:]

    linux_user = 'p000505'
    ambari_server_host = '192.168.0.5'
    remote_host_list = ['192.168.0.3', '192.168.0.5', '192.168.0.7',]

    # preparing for backup operations
    base_dir = osu.get_current_source_contained_path()
    upper_base_dir = osu.get_upper_layer_dir_path(base_dir)
    external_hdd_volume_name = 'Elements'
    external_hdd_drive_letter = osu.find_drive_by_volume_name(external_hdd_volume_name)
    external_hdd_backup_folder = '{0}:/PySong2_BAK_{1}/'.format(external_hdd_drive_letter, today)
    # WinScp getting Linux side ,cpp for putting into backup hdd.
    ubu.file_transfer_linux_remote_to_win_local(ambari_server_host, '/home/{0}/projects/Python3/*'.format(linux_user), '{0}/Linux側コード/Python3/'.format(upper_base_dir))
    # make backup of project related source files
    from_folder_list = ['{0}/eSignal'.format(upper_base_dir), '{0}/PythonUtility'.format(upper_base_dir), '{0}/PyHadoop'.format(upper_base_dir), 'G:/IdeaProjects/flink-quickstart-java/', '{0}/Linux側コード/'.format(upper_base_dir), '{0}/CppKimLinux'.format(upper_base_dir), '{0}/CppBatchLinux'.format(upper_base_dir)]
    to_folder_list = ['{0}/eSignal'.format(external_hdd_backup_folder), '{0}/PyUtility'.format(external_hdd_backup_folder), '{0}/PyHadoop'.format(external_hdd_backup_folder), '{0}/flink-quickstart-java'.format(external_hdd_backup_folder), '{0}/Linux側コード'.format(external_hdd_backup_folder), '{0}/CppKimLinux'.format(external_hdd_backup_folder), '{0}/CppBatchLinux'.format(external_hdd_backup_folder)]
    target_file_wildcard = ['*.py', '*.py', '*.py', '*.java', '', '*.cpp *.h', '*.cpp *.h']
    # need elevated privileage to run Robocopy and UAC poo-up shows up everytime
    need_backup_to_external = param[3] if len(param)==4 else False # 引数4つ目
    if need_backup_to_external or dt.datetime.today().isoweekday() == 6: # 土曜日に週一で強制バックアップ        
        osu.folder_backup_to_hdd(from_folder_list, to_folder_list, target_file_wildcard) # comment-out whnen you need to back up files to external hdd    

    # main function starts    
    print("引数の総個数 = {0}".format(len(param)))
    #if len(sys.argv) != 2:
    #    print("引数を正しく指定してください！")
    for i,x in enumerate(param):
        print("{0}番目の引数 = {1}\n".format(i, x))

    time.sleep(5) # sec

    # parameter : 0=this.py 1=exchange, (2=need_hive_stat)
    #if len(param) == 1: # 引数なし
    # assign parameter based on this process's current execution time
    # opening hours : https://en.wikipedia.org/wiki/List_of_stock_exchange_opening_times
    exchange_time_mapping_dict = { 1915:'TSE',     1945:'TOCOM', 2045:'SHG',  2130:'HKG',  2300:'ASX',    200:'Bombay',  545:'Luxenberg', 615:'BALTIC', 800:'EQUIDUCT', 725:'OMX', 740:'LME', # evening part
                                    1045:'FUTURES',  1015:'AMEX',   1115:'NASDAQ', 1215:'SP500', 1410:'ARCA',   1630:'FUNDS',  # morning part
                                    }
    # BALTIC, FUNDS : task schedler でトリガー削除済

    #print(hour * 100 + minute)
    exhcnage_whose_raw_tick_data_to_be_processed = None
    time_as_int = hour * 100 + minute
    for time_range in range(time_as_int, time_as_int - 6, -1): # in a 5 minute time interval
        try:
            exhcnage_whose_raw_tick_data_to_be_processed = exchange_time_mapping_dict[time_range] # yield error when no matched key
        except:
            None

    exchange = 'Stuttgart' if (exhcnage_whose_raw_tick_data_to_be_processed is None) else exhcnage_whose_raw_tick_data_to_be_processed 
    
    # 引数 1つ目
    if len(param) >= 2: # 引数あり
        if param[1]!="True" and param[1]!="False":
            exchange = param[1]  #'ASX'
        else:
            need_hive_stat = param[1] # exchange 指定ないケース <- scheduler batch 起動
    
    # 引数 2つ目
    need_hive_stat = param[2] if len(param)==3 else False

    designated_remote_host = param[3] if len(param)==4 else None
    for index,exchange in enumerate([exchange]): #exchs.iterrows():

        # estimate individual tickers' tick size from its quote
        if not iou.file_exist("G:/QTickSize/" + exchange + '.csv'):
            calc_individual_tick_size(exchange)

        start_now = dt.datetime.now()

        # 転送予定ファイルを既に持つPCがあれば、優先的に使用する
        q0 = queue.Queue()
        t0 = threading.Thread(target=check_if_any_remote_machine_already_has_the_csv_data, args=[remote_host_list, linux_user, exchange, q0],name="thread Overlapping Files...")
        t0.start()
        prioritized_remote_host = q0.get()

        # 計算先をCPU空％で決定        
        if designated_remote_host is None and prioritized_remote_host is None:
            #determine_most_resourceful_remote_machine(designated_remote_host) # single-threaded version function call
            q1 = queue.Queue()       
            t1 = threading.Thread(target=ubu.determine_most_resourceful_remote_machine, args=[ambari_server_host, remote_host_list, linux_user, 30, q1],name="thread CPU %...")
            t1.start()

        # ======== 作業用フォルダの準備 ======== 
        ftu.print_stdout_with_color('======== 作業用フォルダの準備 ========\n', 'blue', 'yellow')
        tick_folder = 'G:/QCollector Expert For eSignal/{0}/'.format(exchange)
        zipped_folder = tick_folder + 'zipped/'
        kaiseki_folder = 'G:/QLevel1Data/{0}/'.format(exchange)
        simdata_folder = 'G:/QSimulationData/{0}/'.format(exchange)
        iou.create_folder_if_not_exist(zipped_folder)
        iou.create_folder_if_not_exist(kaiseki_folder)
        iou.create_folder_if_not_exist(simdata_folder)

        # zipped フォルダを日付毎に整理
        #reorganize_files_in_the_zipped_folder(zipped_folder) # Windows side
        t2 = threading.Thread(target=reorganize_files_in_the_zipped_folder, args=[zipped_folder],name="thread Zipped...")
        t2.start()
        # QLevel1Data フォルダを日付毎に整理
        #reorganize_files_in_the_gz_data_folder(kaiseki_folder) # Windows side
        t3 = threading.Thread(target=reorganize_files_in_the_gz_data_folder, args=[kaiseki_folder],name="thread 解析用...")
        t3.start()
        # QSimulationData フォルダを日付毎に整理
        # ... to do

        # 銘柄毎の tick size を求める <- Simulation で必須となる
        calc_individual_tick_size(exchange)

        # 上書き防止のため、名前に取得日時文字列を付加
        #add_suffix_to_0_csv_file(tick_folder)
        t4 = threading.Thread(target=add_suffix_to_0_csv_file, args=[tick_folder],name="thread Renaming...")
        t4.start()
        # send Win local files onto Linux
        if designated_remote_host is None and prioritized_remote_host is None:
            t1.join() # Linux remote machine determined
            available_remote_host = q1.get()
            target_remote_host = available_remote_host
        else:
            target_remote_host = designated_remote_host if not designated_remote_host is None else prioritized_remote_host

        msg = 'データ分割 {0} {1} {2}'.format(exchange, target_remote_host, start_now)
        iou.console_title(msg, 'shift-jis')

        # copy the latest C++, Java, python programs into the remote host before proceed.
        coping_program_list = ['/home/{0}/projects/Java/'.format(linux_user),'/home/{0}/projects/Python3/'.format(linux_user), '/home/{0}/projects/CppBatch/bin/x64/Release/'.format(linux_user), '/home/{0}/projects/CppKimG++/bin/x64/Release/'.format(linux_user), '/home/{0}/HiveStat/existing_hive_tables.txt'.format(linux_user)]
        if target_remote_host != ambari_server_host:
            for from_directory in coping_program_list:
                to_directory = from_directory
                #ubu.ubuntu_shell_command(available_remote_host, _linux_user, 'rm -rf {0}'.format(to_directory)) # remove to make sure new directory being copied to
                ubu.ubuntu_transfer_file_linux_to_linux(target_remote_host, from_directory, to_directory, linux_user, ambari_server_host)

        # ======== 2.1 Linux側へ送信済み .csv ファイルリスト取得 ========
        ftu.print_stdout_with_color('======== Linux側へ送信済み .csv ファイルリスト取得  ========\n', 'blue', 'yellow')
        win_file_path = 'G:/QCollector Expert For eSignal/{0}/*_0*.csv'.format(exchange)
        linux_destination_directory_path = '/home/{0}/QCollector_Expert_For_eSignal/{1}/'.format(linux_user, exchange)
        ubu.ubuntu_shell_command(target_remote_host, linux_user, 'mkdir -p {0}'.format(linux_destination_directory_path)) # make directory if not exists
        # exclude existing files
        linux_shell = 'plink -pw M9rgan?? {0}@{1} ls /home/{0}/QCollector_Expert_For_eSignal/{2}/*_0.csv"'.format(linux_user, target_remote_host, exchange)
        result = subprocess.Popen(linux_shell, shell=True, stdout=subprocess.PIPE)    # stdout : http://docs.python.jp/3/library/subprocess.html
        existing_original_tick_files_on_linux = [line.decode('utf-8').replace('\n','').split(exchange+'/')[1] for line in result.stdout]

        t4.join() # tick .csv files rename must be done by here
        
        # ======== 2.2 Linux側受信済み .csv ファイルは .zip 圧縮してWindows側で別フォルダに保管 ========
        ftu.print_stdout_with_color('======== Linux側受信済み .csv ファイルは .zip 圧縮してWindows側で別フォルダに保管  ========\n', 'blue', 'yellow')
        #print(existing_original_tick_files_on_linux)
        for file in existing_original_tick_files_on_linux:
            file = tick_folder + file
            if file.endswith('_0.csv'): # don't touch newly coming in downloaded data 
                continue
            if iou.file_exist(file):
                print('重複ファイル {0} 避難中...'.format(file))
                last_update = dpu.date_to_str(osu.get_file_lastupdate(file))
                zip = file.replace('.csv','.zip')
                zpu.zip_file(tick_folder, file.split('/')[-1], zip.split('/')[-1], True, 'zip') # 将来削除する予定のファイルなので、7zip 高圧縮でなくてよい
                iou.create_folder_if_not_exist(tick_folder + 'to_be_deleted_in_the_future/' + last_update + '/')        
                iou.move_file(zip, zip.replace('/'+exchange+'/', '/'+exchange+'/to_be_deleted_in_the_future/' + last_update + '/'))            

        # ========== 3. Linuxへ.csvデータを転送 ==========
        ftu.print_stdout_with_color('======== Linuxへ.csvデータを転送 ========\n', 'blue', 'yellow')
        ubu.file_transfer_win_local_to_linux_local(win_file_path, target_remote_host, linux_destination_directory_path)
        # ========== 4. 転送済み _0.csv ファイルを .zip 圧縮して保管 ==========
        #print('======== 転送済み _0.csv ファイルを .zip 圧縮して保管  ========')
        ftu.print_stdout_with_color('======== 転送済み _0.csv ファイルを .zip 圧縮して保管 ========\n', 'blue', 'yellow')
        for csv_file in iou.getfiles(tick_folder,'_0.csv'):

            if not csv_file.count('@') == 2:    # ファイル名に日付を付加されていないファイルは圧縮しないでスキップ　⇒ まだ、eSignal データダウンロード途中の可能性
                continue

            print('完了ファイル {0} 避難中...'.format(csv_file))
            csv_file = (tick_folder + '/' + csv_file).replace('//','/').replace('\\','/')
            last_update = dpu.date_to_str(osu.get_file_lastupdate(csv_file))

            zip = csv_file.replace('.csv','.zip')
            zpu.zip_file(tick_folder, csv_file.split('/')[-1], zip.split('/')[-1], True)            
            iou.create_folder_if_not_exist(tick_folder + 'to_be_deleted_in_the_future/' + last_update + '/')
            iou.move_file(zip, zip.replace('/'+exchange+'/', '/'+exchange+'/to_be_deleted_in_the_future/' + last_update + '/'))
            #osu.delete_file(csv_file)
            # compress
            #zip_file = csv_file.replace('.csv','.zip')
            #zpu.zip_file(tick_folder, csv_file, zip_file, True)

        # ========== 5. Linux上で データ分割(DailyTick).py を実行 ==========
        ftu.print_stdout_with_color('======== Linux上で データ分割(DailyTick).py を実行 ========\n', 'blue', 'yellow')
        # list all python version : http://askubuntu.com/questions/505081/what-version-of-python-do-i-have
        result = None
        need_hive_stat = False # HiveでDaily統計を計算する必要が有か否か ⇒ True : 時間かかる, False : スキップする
        # > sudo find / -type f -executable -iname 'python*' -exec file -i '{}' \; | awk -F: '/x-executable; charset=binary/ {print $1}' | xargs readlink -f | sort -u | xargs -I % sh -c 'echo -n "%: "; % -V'
        linux_shell = 'plink -pw M9rgan?? {0}@{1} /home/p000505/anaconda3/bin/python3.5m /home/{0}/projects/Python3/eSignal/SplitRawTicks_Linux.py {2} {3}"'.format(linux_user, target_remote_host, exchange, need_hive_stat)
        #print("==================== Linux上で データ分割(DailyTick).py を実行 ====================== ")
        ftu.print_stdout_with_color('======== Linux上で データ分割(DailyTick).py を実行 ========\n', 'blue', 'yellow')
        print(linux_shell)
        try:
            result = subprocess.run(linux_shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html
        except:
            exit(0)    

        t2.join()
        t3.join()
        # ======== reorganize windows side zipped/ tick_folder ========
        print('======== reorganize windows side zipped/ tick_folder ========')
        for zip_file in iou.getfiles(tick_folder+'zipped','.zip'):
            # 日付部分抽出
            try:
                iso_dt = zip_file.split('_0_')[1].replace('.zip','')
            except:
                print('exception @ moving zip',zip_file)
                continue
            # ファイル移動
            org_dir = tick_folder+'zipped/'
            dst_dir = tick_folder+'zipped/'+iso_dt+'/'
            osu.create_folder_if_not_exist(dst_dir)
            iou.move_file(org_dir + zip_file, dst_dir + zip_file, True)

        # reorganize windows side QLevel1Data/ tick_folder
        print('======== reorganize windows side QLevel1Data/ tick_folder ========')
        tick_folder = 'G:/QLevel1Data/{0}/'.format(exchange)
        for gz_file in iou.getfiles(tick_folder,'.gz'):
            # 日付部分抽出
            try:
                iso_dt = gz_file.split('_0_')[1].replace('_utf8.csv.gz','')
            except:
                print(gz_file)
                continue
            # ファイル移動
            org_dir = tick_folder
            dst_dir = tick_folder+iso_dt+'/'
            osu.create_folder_if_not_exist(dst_dir)
            iou.move_file(org_dir+gz_file, dst_dir+gz_file, True)

    #i = 0
    #m_thread = []
    #for index,exch in exchs.iterrows(): # a stopiteration was not handled exception will be thrown and just ignore it : http://qiita.com/gyu-don/items/0f38dfb48fc7dabbb2ae

    #    now = dt.datetime.now()
    #    msg = 'データ分割中 - with exchange {0}'.format(exch['exchange'])
    #    iou.console_title(msg + ' ' + str(now), 'shift-jis')

    #    tick_folder = 'G:/QCollector Expert For eSignal/{0}/'.format(exch['exchange'])
    #    files = iou.getfiles(tick_folder,'_0.csv')
    #    files1,files2 = dcu.split_list_in_half(files, True)
    #    if files1:  # check the existance of files
    #        t1 = threading.Thread(target=divide_to_day2day, args=[tick_folder,files1,'07:00:00','06:59:59'],name="thread{0}.1".format(i))
    #        m_thread.append(t1)
    #        t1.start()
    #        i += 1
    #    if files2:  # check the existance of files
    #        t2 = threading.Thread(target=divide_to_day2day, args=[tick_folder,files2,'07:00:00','06:59:59'],name="thread{0}.2".format(i))
    #        m_thread.append(t2)
    #        t2.start()
    #        i += 1

    #print('')
    #for worker in m_thread:
    #    print('joining {0}'.format(worker.getName()))   
    #    worker.join()

    #if not need_hive_stat:

    #    # Dailyに分割されたticksを統計処理用に加工
    #    import subprocess
    #    win_shell = "python C:/Users/steve/Desktop/PySong2/eSignal/データ加工.py {0}".format(exchange)
    #    print(win_shell,'\n') # to check what been passed to win_shell
    #    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    #    returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

    #    # Hiveにデータ転送し、集計結果をWinローカルに保存
    #    import subprocess
    #    win_shell = "python C:/Users/steve/Desktop/PySong2/PyHadoop/PyInjection.py 2016-11-01 2017-02-28 {0}".format(exchange)
    #    print(win_shell,'\n') # to check what been passed to win_shell
    #    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    #    returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

    body_text = 'Operation started at {0}'.format(start_now)
    wbu.send_gmail('{0} {1} 完了通知【{2}】'.format(this_file_name.replace('.py',''), exchange, dtu.get_currnet_datetime_str()), body_text)
    print('Leaving ... {0} main thread ... !'.format(frameinfo.filename))

    ## http://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe
    #for index,exch in exchs.iterrows():
    #    tick_folder = 'F:/QCollector Expert For eSignal/{0}/'.format(exch['exchange'])        
    #    for file in iou.getfiles(tick_folder,'_0.csv'):
    #        file = tick_folder + file
    #        divide_to_day2day(file,exch['open'],exch['close'])