# -*- coding:shift-jis -*-
import ptvsd
ptvsd.enable_attach('my_secret')
#raw_input("pause")
import time
time.sleep(20)

import getpass
user = getpass.getuser()

from numba import jit

import os
currentWorkingDirectory = "/home/{0}/projects/Python3/eSignal/".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PyUtility/')
sys.path.append('../PyHadoop/')
print(sys.version_info)

import ctypes

import datetime as dt
#import quandl as ql
import pandas as pd
import queue
import csv

#from bs4 import BeautifulSoup

import IoUtility as iou
import OsUtility as osu
import HolidayUtility as hdu
import DataTypeUtility as dpu
import DatetimeUtility as dtu
import DataCollectionUtility as dcu
import FormatUtility as fmu
import PyHive as hvu
import PyUbuntu as ubu
import WebUtility as wbu

import time
import threading
import subprocess

total_files = 0 # global var
finished_files = 0 # global_var

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

def Cppデータ加工(_exchange):
    import subprocess
    #win_shell = "/home/{0}/projects/CppBatch/bin/x64/Release/CppBatch.a {1}".format(user, _exchange)
    org_cpp_exe1 = "/home/p000505/projects/CppBatch/bin/x64/Debug/CppBatch.out"
    org_cpp_exe2 = "/home/p000505/projects/CppBatch/bin/x64/Release/CppBatch.out"
    if iou.file_exist(org_cpp_exe1):
        org_cpp_exe = org_cpp_exe1
    elif iou.file_exist(org_cpp_exe2):
        org_cpp_exe = org_cpp_exe2
    else:
        print('CppBatch.out does not exists...')
        print('exiting main thread')
        # ToDo : write to log
        exit(0)
    renamed_cpp_exe = org_cpp_exe.replace('CppBatch.out','CppBatch_{0}.out'.format(_exchange)) # rename the original .out file so that this process can be tracked by other sub-thread loop
    print(org_cpp_exe)
    print(renamed_cpp_exe)
    subprocess.run("cp '{0}' '{1}'".format(org_cpp_exe, renamed_cpp_exe), shell=True, check=True)

    #linux_shell = "{0} {1}".format(renamed_cpp_exe, _exchange)
    #print(win_shell,'\n') # to check what been passed to win_shell
    #cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \->\\
    returncode = subprocess.call([renamed_cpp_exe, _exchange])   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished
    return returncode

# YOU CAN CHECK THE splitList(...) FUNCTION VALIDITY WITH THIS FUNCTION!

#@jit
def divide_to_day2day(_folder, _files, _open, _close):
    
    print('inside func() divide_to_day2day')

    for _file in _files:

        if "#OI" in _file:
            continue

        _file = _file.replace('$','\\$')    # linux shell need a \\ before files starting with $ 
        print('dividing files ...',_file)       

        _file = _folder + _file

        print('Dividing {0} into subfiles'.format(_file))

        if os.name=='posix':

            # cat /home/p000505/GE_H8_0.csv | grep "2016-11-28,0[7-9]" > ~/test_2016-11-28.csv            # cat /home/p000505/GE_H8_0.csv | grep "2016-11-28,[1-2][0-9]" >> ~/test_2016-11-28.csv
            # cat /home/p000505/GE_H8_0.csv | grep "2016-11-29,0[0-6]" >> ~/test_2016-11-28.csv

            print("Dividing file ... ",_file)
            try:
                file_first_line = subprocess.check_output('head -n 1 "{0}"'.format(_file).replace('//','/'), shell=True).decode("utf-8")
                file_last_line = subprocess.check_output('tail -n 1 "{0}"'.format(_file).replace('//','/'), shell=True).decode("utf-8")            
            except:
                print('Exception extrcting head tail line from file ',_file)
                continue

            first_date = dpu.str_to_date(file_first_line.split(',')[0])
            last_date = dpu.str_to_date(file_last_line.split(',')[0])

            day_count = dtu.diff(first_date, last_date) + 2

            for session_date in (first_date + dt.timedelta(n) for n in range(day_count)):  # http://stackoverflow.com/questions/1060279/iterating-through-a-range-of-dates-in-python                           

                # this strictly needs you to install workalendar package to the right python kernel !!
                # >sudo /home/p000505/anaconda3/bin/python3.5m `which pip` install workalendar
                # otherwise unsplit _0.csv files will moved to AnalysisData directory will could be problematic for the following procedures...
                if hdu.is_a_holiday('Japan', session_date): # this will limit the session days to be from Monday to Friday (excluding wrongly split file as a Saturday session starting in the morning)
                    continue

                #print('session_date = ',session_date)

                session_overnight_date = dtu.date_add(session_date, 1)
                try:
                    output_file = _file.split('@')[0] + _file.split('@')[2] # ORIIOL-ASX_@2017-07-30 090305@_0_2017-06-16_utf8.csv
                except:
                    print('exception splitting file name ... ')
                output_file = output_file.replace('__','_')
                output_file = output_file.replace('_0.csv', '_0_{0}.csv'.format(session_date))

                #print('output_file = ',output_file)

                #print('spliting file',_file,session_date,'07:00:00 -', session_overnight_date,'06:59:59')

                # extract data with the sepecified time span from the original raw tick files _0.csv
                linux_shell_1 = 'cat "{0}" | grep "{1},0[7-9]" > "{2}"'.format(_file, session_date, output_file) # Monday - Friday 07AM to 09AM
                linux_shell_2 = 'cat "{0}" | grep "{1},[1-2][0-9]" >> "{2}"'.format(_file, session_date, output_file) # Monday - Friday 10AM - 23PM
                linux_shell_3 = 'cat "{0}" | grep "{1},0[0-6]" >> "{2}"'.format(_file, session_overnight_date, output_file) # Monday - Saurday 0AM - 6AM

                #print(linux_shell_1)

                try:
                    #result = subprocess.run(linux_shell_1, shell=True, check=True) # this gives a run-time error when no matching lines
                    result = subprocess.call(linux_shell_1, shell=True)
                except:
                    err = 1   # do nothing
                    raise RuntimeError()
                try:
                    #result = subprocess.run(linux_shell_2, shell=True, check=True) # this gives a run-time error when no matching lines
                    result = subprocess.call(linux_shell_2, shell=True)
                except:
                    err = 2   # do nothing
                    raise RuntimeError()
                try:
                    #result = subprocess.run(linux_shell_3, shell=True, check=True) # this gives a run-time error when no matching lines
                    result = subprocess.call(linux_shell_3, shell=True)
                except:
                    err = 3   # do nothing
                    raise RuntimeError()
                #except:
                #    print('No data was found for {0}'.format(linux_shell_3))

                # remove the generated file if it's empty
                #if(osu.get_file_size(output_file)<=500):    # file under 500 bytes
                #    print('Deleting file {0}'.format(output_file))
                #    osu.delete_file(output_file)

        # finished dividing into day-to-day tick files
        # let's remove the original file
        _file = _file.replace('\\$', '$')   # python shell recognize $ as $. remove the \\ appended in front of the $ 
        #iou.removeReadOnly(_file)
        iou.delete_file(_file)

def calc_flink_stat(_exchange): #, _csv_files, _gz_files): # execute flink Java program as a Linux command

    print("Inside CALC_FLINK_STAT")
    jar_program = '/home/p000505/projects/Java/flink-quickstart-java-1.4-SNAPSHOT.jar'
    entry_class = 'songyu.flink.test.MPAnalysis'

     # _csv_files : .csv files in the QCollector_Exprt_For_eSignal/TSE/ dir which are being converted into .gz cnocurrently by another thread
     # _gz_files  : .gz files which are already ready in the AnalysisData/TSE/ directory
#     gz_files = [f.replace('.csv','_utf8.csv.gz') for f in _csv_files]
#     print('num of .gz files = ',len(gz_files + _gz_files))
    gz_dir = '/home/p000505/AnalysisData/{0}/gzed/'.format(_exchange)
    subprocess.run("mkdir -p '{0}'".format(gz_dir), shell=True, check=True)
    gz_list = iou.getfiles('/home/p000505/AnalysisData/{0}/'.format(_exchange),'.gz')
    is_CppBatch_process_running = True
    print('number of .gz files = {0}'.format(len(gz_list)))
    while gz_list or is_CppBatch_process_running: # as long as .gz files exist
        
        for gz_file in gz_list: # merge the .gz candidates list in both directories
            input_file = "/home/p000505/AnalysisData/{0}/{1}".format(_exchange, gz_file)
            if not os.path.isfile(input_file):
                print(input_file,' does not exist')
                continue
            non_shell_linux_command = "/home/p000505/flink-1.3.1/bin/flink run -c {0} {1} --input {2} --output /home/p000505/AnalysisData/{3}/FLINK/".format(entry_class, jar_program, input_file, _exchange)
            print(non_shell_linux_command)
            os.system(non_shell_linux_command) # https://stackoverflow.com/questions/3345202/getting-user-input
            subprocess.run("mv {0} {1}".format(input_file, gz_dir), shell=True, check=True)

        gz_list = iou.getfiles('/home/p000505/AnalysisData/{0}/'.format(_exchange),'.gz')
        is_CppBatch_process_running = osu.is_process_with_cmdline_exist('CppBatch_{0}.out'.format(_exchange)) # check whether CppBatch process for this exchange is running
        print('is_CppBatch_prc_running ? : ',is_CppBatch_process_running)

# divide tick files with multiple days' tick into single day's tick
if __name__ == '__main__':

    print('============== Inside SplitRawTicks_Linux.py @ {0} : {1} {2} ================'.format(os.uname()[1], len(sys.argv), sys.argv[2]))

    start_now = dt.datetime.now()
    hour = start_now.hour
    minute = start_now.minute

    #print("引数の総個数 = {0}\n".format(len(sys.argv)))
    #if len(sys.argv) != 2:
    #    print("引数を正しく指定してください！")

    #for i,x in enumerate(sys.argv):
    #    print("{0}番目の引数 = {1}\n".format(i, x))

    # parameter : 0=this.py 1=exchange, (2=need_hive_stat)
    param = sys.argv

    print("引数の総個数 = {0}".format(len(param)))
    #if len(sys.argv) != 2:
    #    print("引数を正しく指定してください!")
    for i,x in enumerate(param):
        print("{0}番目の引数 = {1}\n".format(i, x))

    # parameter : 0=this.py 1=exchange, (2=need_hive_stat)
    if len(param) == 1: # 引数なし

        exchange = None
    
    # 引数1つ目
    if len(param) >= 2: # 引数あり
        exchange = param[1]  #'ASX'
    
    # 引数2つ目
    need_hive_stat = param[2] if len(param)==3 else False

    # 引数3つ目
    need_local_to_hdfs = param[3] if len(param)==4 else False

    print(exchange)
    if exchange is None:
        exit(0)

    m_thread = []
    for index,target_exchange in enumerate([exchange]): # a stopiteration was not handled exception will be thrown and just ignore it : http://qiita.com/gyu-don/items/0f38dfb48fc7dabbb2ae

        print('=======================')

        msg = 'データ分割中 - with exchange {0}'.format(target_exchange)

        # rename files before dividing raw files
        linux_cmd = '/home/p000505/QCollector_Expert_For_eSignal/rename_eSignal_raw.sh {0}'.format(target_exchange)
        res = subprocess.run(linux_cmd,shell=True) # doesn't go well with Popen()

        start = time.time() # measure start time

        #thread_num = 12
        folder = '/home/{0}/QCollector_Expert_For_eSignal/{1}/'.format(user, target_exchange)
        files = iou.getfiles(folder,'_0.csv')
        print('num of files = ',len(files))
        #global total_files
        total_files = len(files)
        if total_files!=0:

            thread_num = min(12,len(files))
#            print(len(files),thread_num)
            bin_size = len(files) // thread_num
            files_sublist = dcu.splitList(files, bin_size)
            for i, sublist in enumerate(files_sublist):
                if sublist: # check the existance of files
#                    divide_to_day2day(folder,files_sublist[i],'07:00:00','06:59:59')
                     t = threading.Thread(target=divide_to_day2day, args=[folder,files_sublist[i],'07:00:00','06:59:59'],name="thread{0} {1}".format(i, target_exchange))
                     m_thread.append(t)
                     t.start()

            print('')
            for worker in m_thread:
                print('joining {0}'.format(worker.getName()))   
                worker.join()    

            end = time.time() # measure finish time
            print("file-splits takes ", fmu.format_decimal(end - start, 5), " sec")
        
#        try:
#            hive_stat_exist = subprocess.check_output('ls -R /home/p000505/HiveStat/ | grep "{0}"'.format(exch), shell=True).decode("utf-8")
#        except:
#            hive_stat_exist = 'dummy_to_avoid_hadoop_transaction_20171105'
#
#        print(hive_stat_exist)
#        hive_stat_exist = 'dummy_to_avoid_hadoop_transaction_now_20171105'

#        if hive_stat_exist=='':

#            linux_shell = 'mkdir -p /home/p000505/HiveStat/{0}/'.format(target_exchange)
#            print(linux_shell)
#            result = subprocess.run(linux_shell, shell=True, check=True)
#            log = '/home/p000505/HiveStat/existing_hive_tables.txt'
##            if os.path.exists(log):
##                append_write = 'a' # append if already exists
##            else:
##                append_write = 'w' # make a new file if not
#            with open(log) as f:
#                exsiting_hive_table_str = f.read()  # ファイル終端まで全て読んだのを返す
#            # Define Hive tables (text, orc)
##            is_textfile_hive_table_exist = False
##            is_orc_hive_table_exist = False
#            table_schema = """dt TIMESTAMP,
#                                val FLOAT,
#                                chg FLOAT,
#                                act VARCHAR(10),
#                                exg VARCHAR(10),
#                                smb VARCHAR(30),
#                                bas FLOAT,
#                                mp FLOAT,
#                                id INT"""
#            partition_schema = 'sdt STRING'
#            external_location = '/QCollector/{0}/'.format(target_exchange)
#            m_thread = []
#            # 4.1 create a hive table (textfile)
#            q = queue.Queue() # for passing around the thread function's return value : http://stackoverflow.com/questions/2577233/threading-in-python-retrieve-return-value-when-using-target
#            hive_textfile_table = 'eSignal.{0}_txt'.format(target_exchange)
#            is_textfile_hive_table_exist = True if hive_textfile_table in exsiting_hive_table_str else False # 2017.07.29
#            if not is_textfile_hive_table_exist:
#                # is_textfile_hive_table_exist = hvu.create_hive_table(hive_textfile_table, table_schema, 'TEXTFILE',  partition_schema, external_location + 'text/')
#                t1 = threading.Thread(target=hvu.create_hive_table, args=[hive_textfile_table, table_schema, 'TEXTFILE',  partition_schema, external_location + 'text/', q],name="thread{0}.1".format(1))
#                m_thread.append(t1)
#                t1.start()
#                is_textfile_hive_table_exist = q.get()
#                # put the successfully created Hive table's name onto a txt file # 2017.07.29
#                if is_textfile_hive_table_exist:
#                    osu.create_folder_if_not_exist('/home/p000505/HiveStat/')
#                    with open("/home/p000505/HiveStat/existing_hive_tables.txt", "a") as existing_hive_table_file:
#                        existing_hive_table_file.write("\n{0}".format(hive_textfile_table))

#            # 4.2 create a hive table (orc)
#            q = queue.Queue() # for passing around the thread function's return value : http://stackoverflow.com/questions/2577233/threading-in-python-retrieve-return-value-when-using-target
#            hive_orc_table = 'eSignal.{0}_orc'.format(target_exchange)
#            is_orc_hive_table_exist = True if hive_orc_table in exsiting_hive_table_str else False # 2017.07.29
#            if not is_orc_hive_table_exist:
#                #is_orc_hive_table_exist = hvu.create_hive_table(hive_orc_table, table_schema, 'ORC',  partition_schema, external_location + 'orc/') 
#                t2 = threading.Thread(target=hvu.create_hive_table, args=[hive_orc_table, table_schema, 'ORC',  partition_schema, external_location + 'orc/', q],name="thread{0}.2".format(2))
#                m_thread.append(t2)
#                t2.start()
#                is_orc_hive_table_exist = q.get()
#                # put the successfully created Hive table's name onto a txt file # 2017.07.29
#                if is_orc_hive_table_exist:
#                    osu.create_folder_if_not_exist('/home/p000505/HiveStat/')
#                    with open("/home/p000505/HiveStat/existing_hive_tables.txt", "a") as existing_hive_table_file:
#                        existing_hive_table_file.write("\n{0}".format(hive_orc_table))

#            print('')
#            for worker in m_thread:
#                print('joining {0}'.format(worker.getName()))
#                worker.join()

        # Converting eSignal raw ticks into simulation format
#       returncode = subprocess.call(["/home/p000505/anaconda3/bin/python3.5m", "/home/{0}/projects/Python3/eSignal/TransformRawToSim.py".format(user), target_exchange])   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished
        # @process details, @プロセス情報
        cmdline_search_str = 'データ加工_{0}.bat'.format(target_exchange)
        found = osu.is_process_with_cmdline_exist(cmdline_search_str)
        # 既存プロセスなければ
        if not found:
            Cppデータ加工(target_exchange)

        # calculating cep signals with .gz files as a thread
        #flink_thread = threading.Thread(target=calc_flink_stat, args=[target_exchange],name="thread CALC_FLINK_STAT : {0}".format(target_exchange))
        #flink_thread.start()

        # transfer zipped original raw tick file back to win side <- commented out on 2017.11.07
#        win_pc_ip = '192.168.0.6'
#        win_pc_share_folder = 'QCollector Expert For eSignal'
#        win_pc_share_sub_folder_path = '/{0}/zipped/'.format(target_exchange)
#        linux_from_dir = '/home/{0}/QCollector_Expert_For_eSignal/{1}/zipped/'.format(user, target_exchange)
        # transfer all files in the specified directory in a one session using mput command
#        linux_shell = "smbclient -U steve%M9rgan?? -c 'lcd {0}; recurse on;prompt off;mput *.zip; exit;' '//{1}/{2}' -D '{3}'".format(linux_from_dir, win_pc_ip, win_pc_share_folder, win_pc_share_sub_folder_path)
#        result = subprocess.run(linux_shell, shell=True, check=True)

	# delete files transferred into Windows <- commented out on 2017.11.07
#        linux_shell = "smbclient -U steve%M9rgan?? -c \"ls *.zip; exit;\" '\\\\{0}\\{1}' -D '{2}' ".format(win_pc_ip, win_pc_share_folder, win_pc_share_sub_folder_path)
#        result = subprocess.Popen(linux_shell, shell=True, stdout=subprocess.PIPE)    # stdout : http://docs.python.jp/3/library/subprocess.html
#        transferred_files = []
#        lines = [line.decode('utf-8').replace('\n','').split(',') for line in result.stdout]
#        for line in lines:
#            str = ''.join(line) # list to string
#            if '.zip' in str:
#                file = (str.split('.zip')[0]+'.zip').strip()
                #print(file)
#                transferred_files.append(file)
	# delete transferred files
	#linux_from_dir = '/home/{0}/QCollector_Expert_For_eSignal/{1}/zipped/'.format(user, target_exchange)
#        transferred_files = [f.replace('\$','$') if f[0]=='\\' else f for f in transferred_files] # file name starting with $ needs to be preceeded with a \
#        [os.remove(linux_from_dir + file) if os.path.isfile(linux_from_dir + file) else None for file in transferred_files]

#        for i,f in enumerate(iou.getfiles('/home/{0}/QCollector_Expert_For_eSignal/{1}/zipped'.format(user,target_exchange), '.zip')):
#            f = f.replace('$','\$') if f[0]=='$' else f # file name starting with $ needs to be preceeded with a \
#            linux_from_file = '/home/{0}/QCollector_Expert_For_eSignal/{1}/zipped/{2}'.format(user, target_exchange,f)
#            win_to_file = '{0}\zipped\{1}'.format(target_exchange, f)
#            win_to_file = win_to_file.replace('$','\\$') # windows' side path
#            #display_stdout_or_not = True if (i%500)==0 else False
#            ubu.file_transfer_linux_local_to_win_remote('','', win_pc_ip, win_pc_share_folder, linux_from_file, win_to_file)

#            f = f.replace('\$','$') if f[0]=='\\' else f # file name starting with $ needs to be preceeded with a \
#            linux_from_file = '/home/{0}/QCollector_Expert_For_eSignal/{1}/zipped/{2}'.format(user, target_exchange,f)
#            osu.delete_file(linux_from_file)

    body_text = 'Operation started at {0}'.format(start_now)
    wbu.send_gmail('SplitRawT_Linux {0} 完了通知 【{1}】'.format(exchange, dtu.get_currnet_datetime_str()), body_text)
    print('Leaving ... SplitRawTicks_Linux.py main thread ... !')
