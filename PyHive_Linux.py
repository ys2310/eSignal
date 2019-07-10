# -*- coding:shift-jis -*-

#import numpy as np
#import pandas as pd

#import matplotlib.pyplot as plt
#import seaborn as sns

#sns.pairplot(iris, hue='species');

import sys
import os.path
import ctypes
import queue
import pandas as pd
from datetime import datetime as dt

import subprocess
from subprocess import check_output

import PyUbuntu as ubt

# Some HIVE tips!!
# 
# https://www.cheatography.com/davidpol/cheat-sheets/apache-hive-in-easy-steps/
# Hive is not an RDBMS, but it pretends to be one most of the time.
# It has tables, it runs SQL and it supports both JDBC and ODBC.

# I hope this means my endless late-night-sufferings will not have been in vain.

# 1.Don't use MapReduce
# Whether you believe in Tez, Spark or Impala, don't beleive in MapReduce.
# It's slow on its own, and it's really slow under Hive.
# If you're on Hortonworks' distribution, you can throw "set hive.execution.engine=tez"
# at the top of a script.

# 2.Don't do string matching in SQL
# If you stick a like string match where a clause should be, you'll generate a cross-product warning.
# If you have a query that runs in seconds, with string matching it will take minutes.
# Your best alternative is to use one of many tools that allow you to add search to Hadoop.
# Look at Elasticsearch's Hive integration or Lucidwork's integration for Solr. or Cloudera Search.
# RDBMSes were never good at this, but Hive is worse.

# 3. Don't do a join on a subquery.
# You're better off creating a temporary table, then joining against the temp table instead of 
# asking Hive to be smart about how it handles subqueries. 

# select a.* from something a inner join 
# (select ... from somethingelse 
# union
# b select ... from something c) d on a.key1 = d.key1 and a.key2 = b.key2 where a.condition=1
 
# Instead, do this:
 
# create var_temp as select ... from somethingelse b union select ... from
# anotherthing c and then select a.* from something a inner join from var_temp
# b where a.key1 = b.key1 and a.key2 = b.key2 where a.condition=1
 
# It really shouldn't be tons faster at this point in Hive's evolution, but it
# is, generally.
 
# 4. Try tuning vectorization on and off
# Add 
# "set hive.vectorized.execution.enabled = true"
# "set hive.vectorized.execution.reduce.enabled = true"
# on the top of your scripts.
# Try it with them on and off because vectorizatin seems problematic in 
# recent versions of Hive.
 
# 5. Don't use structs in a join
 
# 6. Check your container size
# You may need to increase your container size for Impala or Tez.
# Also, the "reccomended" sizes may not apply to your system if you have
# larger node sizes. You might want to make sure your YARN queue and general
# YARN memory are appropriate. You might also want to peg it to something that
# isn't the default queue all the peasants use.
 
# 7. Enable statistics
# Hive does somewhat boneheaded things with joins unless statistics are enabled.
# You may also want to use query hints in Impala.
 
# 8. Consider Mapjoin optimizations
# If you do an explain on your query, you may find recent versions of Hive are smart
# enough to apply the optimizatin automatically. But you may need to tweak them.
 
# 9. Partitions are your friends ... sorta
# If you have this one item in many places where clauses like a date (but ideally not a range)
# or a location repeat, you might have your partitin key! 
# However, remember HDFS doesn't love small files.
 
# 10.Use hashes for column comparsions
# If you're comparing the same 10 fields in every query, consider using hash() and 
# comparing the sums. These are sometimes so useful you might shove them in an output table.
# Note that the hash in Hive 0.12 is a low resolution, but better hashes are available in 0.13.
 
def hive_show_table_partitions(_table_name): # the partitioned data are stored under the hdfs /user/hive/warehose/... directories

    if os.name=='nt': # Windows
        win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"SHOW PARTITIONS {0}\\\"'".format(_table_name)
    elif os.name=='posix': # Linux
        win_shell = "sudo su - hive -c 'hive -e \\\"SHOW PARTITIONS {0}\\\"'".format(_table_name)

    print(win_shell,'/n') # to check what been passed to win_shell
    cmd = win_shell
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

def hive_show_tables_in_db(_db_name): # the partitioned data are stored under the hdfs /user/hive/warehose/... directories

    if os.name=='nt': # Windows
        win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"SHOW TABLES IN {0}\\\"'".format(_db_name)
    elif os.name=='posix': # Linux
        win_shell = "sudo su - hive -c 'hive -e \\\"SHOW TABLES IN {0}\\\"'".format(_db_name)

    print(win_shell,'/n') # to check what been passed to win_shell
    cmd = win_shell
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

def hive_describe_table(_table_name, _is_formatted=True):
    if _is_formatted:
        if os.name=='nt': # Windows
            win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"DESCRIBE FORMATTED {0}\\\"'".format(_table_name)
        elif os.name=='posix': # Linux
            win_shell = "sudo su - hive -c 'hive -e \\\"DESCRIBE FORMATTED {0}\\\"'".format(_table_name)
    else:
        if os.name=='nt': # Windows
            win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"DESCRIBE EXTENDED {0}\\\"'".format(_table_name)
        elif os.name=='posix': # Linux
            win_shell = "sudo su - hive -c 'hive -e \\\"DESCRIBE EXTENDED {0}\\\"'".format(_table_name)

    print(win_shell,'/n') # to check what been passed to win_shell
    cmd = win_shell
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished    

def hive_list_of_builtin_func():
    if os.name=='nt': # Windows
        win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"SHOW FUNCTIONS\\\"'"
    elif os.name=='posix': # Linux
        win_shell = "sudo su - hive -c 'hive -e \\\"SHOW FUNCTIONS\\\"'"
    print(win_shell,'/n') # to check what been passed to win_shell
    cmd = win_shell
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished    

def hive_describe_func(_func_name):

    if os.name=='nt': # Windows
        win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"DESCRIBE FUNCTION EXTENDED {0}\\\"'".format(_func_name)
    elif os.name=='posix': # Linux
        win_shell = "sudo su - hive -c 'hive -e \\\"DESCRIBE FUNCTION EXTENDED {0}\\\"'".format(_func_name)

    print(win_shell,'/n') # to check what been passed to win_shell
    cmd = win_shell
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished    

# create a hive external table with partition in textfile format
def create_hive_table(_table_name, _table_schema, _table_format,  _partition_schema, _external_location='', _synchronised_queue=queue.Queue()):
    
    # drop the existing table
    #drop_cmd = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"DROP TABLE {0}\\\"'".format(_table_name)
    #subprocess.call(drop_cmd)

    # Linux shell 上では以下の文法規則が正しい入力である
    # point は BY の後に "," となっていることですが、外が既に" "で囲まれているので、\" にする必要がある。
    # また、Python の文字列型変数として扱っているため、" "が二重となっていて、\\\" で " を表す必要がある点が要注意。
    #
    # sudo su - hdfs -c 'hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS eSignal.SHG_orc(dt TIMESTAMP,
	#                     val FLOAT,
	#                     chg FLOAT,
	#                     act VARCHAR(10),
	#                     exg VARCHAR(10),
	#                     smb VARCHAR(30),
	#                     bas FLOAT,
	#                     mp FLOAT,
	#                     id INT) PARTITIONED BY(sdt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\" LINES TERMINATED BY \"\n\" STORED AS ORC LOCATION \"/QCollector/SHG/orc/\" tblproperties(\"skip.header.line.count\"=\"1\")"'
                           
    #win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e '\\''CREATE EXTERNAL TABLE IF NOT EXISTS crd.parameter_txt16(name STRING, param DOUBLE) PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\"'\\''\\\",\\\"'\\''\\\" LINES TERMINATED BY \\\"'\\''\\\"\\n\\\"\'\\''\\\" STORED AS TEXTFILE'\\'' LOCATION '\\''/ft/crd/'\\'''"  
    if _external_location != '': # external table
        
        # correct the external location path format so that hiveSql recognizes it
        _external_location = _external_location.replace('\\\\', '\\')
        _external_location = _external_location.replace('\\', '/')

        if _partition_schema != '':                 # with partition
            if os.name=='nt': # Windows
                win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"CREATE EXTERNAL TABLE IF NOT EXISTS {0}({1}) PARTITIONED BY({3}) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\'','\\'' LINES TERMINATED BY '\\''\\n\'\\'' STORED AS {2} LOCATION '\\''{4}'\\'' tblproperties('\\''skip.header.line.count'\\''='\\''1'\\'') \\\"'".format(_table_name, _table_schema, _table_format, _partition_schema, _external_location)
            elif os.name=='posix': # Linux
                win_shell = "sudo su - hdfs -c 'hive -e \"CREATE EXTERNAL TABLE IF NOT EXISTS {0}({1}) PARTITIONED BY({3}) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" LINES TERMINATED BY \\\"\\n\\\" STORED AS {2} LOCATION \\\"{4}\\\" tblproperties(\\\"skip.header.line.count\\\"=\\\"1\\\")\"'".format(_table_name, _table_schema, _table_format, _partition_schema, _external_location)
        else:                                       # no partition
            if os.name=='nt': # Windows
                win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"CREATE EXTERNAL TABLE IF NOT EXISTS {0}({1}) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\'','\\'' LINES TERMINATED BY '\\''\\n\'\\'' STORED AS {2} LOCATION '\\''{3}'\\'' tblproperties('\\''skip.header.line.count'\\''='\\''1'\\'') \\\"'".format(_table_name, _table_schema, _table_format, _external_location)
            elif os.name=='posix': # Linux
                win_shell = "sudo su - hdfs -c 'hive -e \"CREATE EXTERNAL TABLE IF NOT EXISTS {0}({1}) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" LINES TERMINATED BY \\\"\\n\\\" STORED AS {2} LOCATION \\\"{3}\\\" tblproperties(\\\"skip.header.line.count\\\"=\\\"1\\\") \"'".format(_table_name, _table_schema, _table_format, _external_location)
    else:                        # internal table
        if _partition_schema != '':                 # with partition
            if os.name=='nt': # Windows
                win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"CREATE TABLE IF NOT EXISTS {0}({1}) PARTITIONED BY({3}) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\'','\\'' LINES TERMINATED BY '\\''\\n\'\\'' STORED AS {2} tblproperties('\\''skip.header.line.count'\\''='\\''1'\\'') \\\"'".format(_table_name, _table_schema, _table_format, _partition_schema)
            elif os.name=='posix': # Linux
                win_shell = "sudo su - hdfs -c 'hive -e \"CREATE TABLE IF NOT EXISTS {0}({1}) PARTITIONED BY({3}) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" LINES TERMINATED BY \\\"\\n\\\" STORED AS {2} tblproperties(\\\"skip.header.line.count\\\"=\\\"1\\\") \"'".format(_table_name, _table_schema, _table_format, _partition_schema)
        else:                                       # no partition
            if os.name=='nt': # Windows
                win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"CREATE TABLE IF NOT EXISTS {0}({1}) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\'','\\'' LINES TERMINATED BY '\\''\\n\'\\'' STORED AS {2} tblproperties('\\''skip.header.line.count'\\''='\\''1'\\'') \\\"'".format(_table_name, _table_schema, _table_format)
            elif os.name=='posix': # Linux
                win_shell = "sudo su - hdfs -c 'hive -e \"CREATE TABLE IF NOT EXISTS {0}({1}) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" LINES TERMINATED BY \\\"\\n\\\" STORED AS {2} tblproperties(\\\"skip.header.line.count\\\"=\\\"1\\\") \"'".format(_table_name, _table_schema, _table_format)

    print(win_shell,'\\n') # to check what been passed to win_shell
    cmd = win_shell #"{0}".format(win_shell).replace('\\\\','\\\\\\\\') # replace single backslash with double backslash \\ ⇒ \\\\
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished
 
    _synchronised_queue.put(True) if returncode == 0 else _synchronised_queue.put(False)
    return True if returncode == 0 else False

# load local data into hive textfile table
def load_linux_local_data_onto_hdfs(_linux_local_file_path, _hdfs_table, _partition_string=''):
    
    # correct the linux local file path format so that hiveSql recognizes it
    _linux_local_file_path = _linux_local_file_path.replace('\\\\', '\\')
    _linux_local_file_path = _linux_local_file_path.replace('\\', '/')
    # correct the conditional partition string format so that hiveSql recognizes it
    _partition_string = _partition_string.replace("'", "'\\''")
    
    # p000505@itubuntu04:$ >                            sudo su - hive -c 'hive -e "LOAD DATA LOCAL INPATH '\''/usr/QCollector/ASX/*2016-12-16*.csv'\'' INTO TABLE eSignal.ASX_txt partition(dt='\''2016-12-16'\'')"'
    #win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e  LOAD DATA LOCAL INPATH '\\''/home/p000505/local/parameter.csv'\\'' INTO TABLE crd.parameter_txt14 partition(dt='\\''20161208'\\'')'"
    if _partition_string != '':
        if os.name=='nt': # Windows
            win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"LOAD DATA LOCAL INPATH '\\''{0}'\\'' OVERWRITE INTO TABLE {1} partition({2}) \\\"'".format(_linux_local_file_path, _hdfs_table, _partition_string)
        elif os.name=='posix': # Linux
            win_shell = "sudo su - hdfs -c 'hive -e \"LOAD DATA LOCAL INPATH \\\"{0}\\\" OVERWRITE INTO TABLE {1} partition({2}) \"'".format(_linux_local_file_path, _hdfs_table, _partition_string)
    else:
        if os.name=='nt': # Windows
            win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"LOAD DATA LOCAL INPATH '\\''{0}'\\'' OVERWRITE INTO TABLE {1} \\\"'".format(_linux_local_file_path, _hdfs_table)
        elif os.name=='posix': # Linux
            win_shell = "sudo su - hdfs -c 'hive -e \"LOAD DATA LOCAL INPATH \\\"{0}\\\" OVERWRITE INTO TABLE {1} \"'".format(_linux_local_file_path, _hdfs_table)

    print(win_shell,'\n') # to check what been passed to win_shell
    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

# insert data from hive textfile table into hive orc table
def insert_hive_data_from_textfile_table_into_orc_table(_textfile_table, _orc_table, _all_fields_string_for_select_query, _partition_string=''):

    # correct the conditional partition string format so that hiveSql recognizes it
    _partition_string = _partition_string.replace("'", "'\\''")

    # To avoid the " SemanticException [Error 10096]: Dynamic partition strict mode requires at least one static partition column. To turn this off set hive.exec.dynamic.partition.mode=nonstrict"
    # http://tagomoris.hatenablog.com/entry/20141114/1415938647
    # http://stackoverflow.com/questions/22006655/one-record-insert-to-hive-partitioned-table
    #cmd_hive_setting1 = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"set hive.exec.dynamic.partition=true\\\"'"
    #cmd_hive_setting2 = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"set hive.exec.dynamic.partition.mode=nonstrict\\\"'"
    #returncode = subprocess.call(cmd_hive_setting1)
    #returncode = subprocess.call(cmd_hive_setting2)

    #win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e INSERT INTO TABLE crd.parameter_orc6 SELECT * FROM crd.parameter_txt6'"    
    #win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"INSERT INTO TABLE {0} PARTITION(dt='\\''2016-12-16'\\'') SELECT datetime, ticker FROM {1}\\\"'".format(_orc_table, _textfile_table)
    if _partition_string != '':
        if os.name=='nt': # Windows
            win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"INSERT OVERWRITE TABLE {0} PARTITION({3}) SELECT {2} FROM {1} WHERE {3}\\\"'".format(_orc_table, _textfile_table, _all_fields_string_for_select_query, _partition_string)
        elif os.name=='posix': # Linux
            win_shell = "sudo su - hdfs -c 'hive -e \"INSERT OVERWRITE TABLE {0} PARTITION({3}) SELECT {2} FROM {1} WHERE {3}\"'".format(_orc_table, _textfile_table, _all_fields_string_for_select_query, _partition_string)
    else:
        if os.name=='nt': # Windows
            win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"INSERT OVERWRITE TABLE {0} SELECT * FROM {1}\\\"'".format(_orc_table, _textfile_table)
        elif os.name=='posix': # Linux
            win_shell = "sudo su - hdfs -c 'hive -e \"INSERT OVERWRITE TABLE {0} SELECT * FROM {1}\"'".format(_orc_table, _textfile_table)

    print(win_shell,'\n') # to check what been passed to win_shell
    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

# basic queries
def hive_query(_hiveSql):
    
    #win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e '\\''SELECT COUNT(*) FROM parameter_orc6 WHERE dt>'\\''20160901'\\'' AND dt<='\\''20161201'\\''''"
    if os.name=='nt': # Windows
        win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"{0}\\\"'".format(_hiveSql)
    elif os.name=='posix': # Linux
        win_shell = "sudo su - hdfs -c 'hive -e \"{0}\"'".format(_hiveSql)

    print(win_shell,'\n') # to check what been passed to win_shell
    cmd = win_shell #"{0}".format(win_shell).replace('\\','\\\\') # replace single backslash with double backslash \ ⇒ \\
    returncode = subprocess.call(cmd, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

def hive_table_stat_youbi_hourly_act_count(_orc_table_name, _start_date, _end_date):

    login_user = 'p000505'
    tsv_file_for_result = "/home/hdfs/youbi_hourly_{0}_{1}_{2}.tsv".format(_orc_table_name,_start_date,_end_date)
    csv_file_for_result = "/home/{3}/HiveStat/youbi_hourly_act_count/youbi_hourly_{0}_{1}_{2}.csv".format(_orc_table_name,_start_date,_end_date, login_user)

    #win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e '\\''SELECT COUNT(*) FROM parameter_orc6 WHERE dt>'\\''20160901'\\'' AND dt<='\\''20161201'\\''''"
    hiveSql = "SELECT act,IF(chg>0, '\\''+'\\'', '\\''-'\\'') as chg,smb,from_unixtime(unix_timestamp(dt),'\\''EEEE'\\'') as sd,hour(dt) as sh, COUNT(*) AS count FROM {0} WHERE sdt BETWEEN '\\''{1}'\\'' AND '\\''{2}'\\'' GROUP BY act,IF(chg>0, '\\''+'\\'', '\\''-'\\''),smb,from_unixtime(unix_timestamp(dt),'\\''EEEE'\\''),hour(dt) ORDER BY act,chg,smb,sd,sh;".format(_orc_table_name, _start_date, _end_date)

    # save the result as a csv file : http://stackoverflow.com/questions/18129581/how-do-i-output-the-results-of-a-hiveql-query-to-csv
    #csv_file_for_result = "/usr/QCollector/HiveStat/youbi_hourly_act_count/{0}_{1}_{2}".format(_orc_table_name,_start_date,_end_date)

    if os.name=='nt': # Windows    
        win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"{0}\\\" > {1}'".format(hiveSql, tsv_file_for_result)
    elif os.name=='posix': # Linux
        win_shell = "sudo su - hdfs -c 'hive -e \"{0}\" > {1}'".format(hiveSql, tsv_file_for_result)

    print(win_shell) # to check what been passed to win_shell    
    print()
    returncode = subprocess.call(win_shell, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished
    # tsv to csv : http://stackoverflow.com/questions/22419979/how-do-i-convert-a-tab-separated-values-tsv-file-to-a-comma-separated-values    
    ubt.ubuntu_move_file(tsv_file_for_result, "/home/{0}/HiveStat/youbi_hourly_act_count/".format(login_user))
    ubt.ubuntu_convert_tsv_file_to_csv_file(csv_file_for_result.replace('csv', 'tsv'), csv_file_for_result)
    ubt.ubuntu_rm_a_file(csv_file_for_result.replace('csv', 'tsv'))

    if os.name=='nt':
        win_folder = 'G:\\HiveStat\\youbi_hourly_act_count\\'
        ubt.file_transfer_linux_local_to_win_remote(csv_file_for_result, win_folder)
    elif os.name=='posix':
        win_pc_ip = '192.168.0.6'
        share_folder = 'HiveStat'
        linux_from_file = csv_file_for_result
        win_to_flie = 'youbi_hourly_act_count\youbi_hourly_{0}_{1}_{2}.csv'.format(_orc_table_name,_start_date,_end_date)
        ubt.file_transfer_linux_local_to_win_remote('', '', win_pc_ip, share_folder, linux_from_file, win_to_flie)

def hive_table_stat_daily_hourly_act_count(_orc_table_name, _start_date, _end_date):

    login_user = 'p000505'
    tsv_file_for_result = "/home/hdfs/daily_hourly_{0}_{1}_{2}.tsv".format(_orc_table_name,_start_date,_end_date)
    csv_file_for_result = "/home/{3}/HiveStat/daily_hourly_act_count/daily_hourly_{0}_{1}_{2}.csv".format(_orc_table_name,_start_date,_end_date, login_user)

    # sudo su - hdfs -c 'hive -e "SELECT act,from_unixtime(unix_timestamp(dt),'\''EEEE'\'') as sd,hour(dt) as sh, COUNT(*) AS count FROM eSignal.asx_orc WHERE sdt BETWEEN '\''2016-12-01'\'' AND '\''2016-12-30'\'' GROUP BY act,from_unixtime(unix_timestamp(dt),'\''EEEE'\''),hour(dt) ORDER BY act,sd,sh;" > /home/hdfs/eSignal_test_20161231.csv'
    hiveSql = "SELECT act,IF(chg>0, '\\''+'\\'', '\\''-'\\'') as chg,smb,day(dt) as sd,hour(dt) as sh, COUNT(*) AS count FROM {0} WHERE sdt BETWEEN '\\''{1}'\\'' AND '\\''{2}'\\'' GROUP BY act,IF(chg>0, '\\''+'\\'', '\\''-'\\''),smb,day(dt),hour(dt) ORDER BY act,chg,smb,sd,sh;".format(_orc_table_name, _start_date, _end_date)

    # save the result as a csv file : http://stackoverflow.com/questions/18129581/how-do-i-output-the-results-of-a-hiveql-query-to-csv
    #csv_file_for_result = "/usr/QCollector/HiveStat/youbi_hourly_act_count/{0}_{1}_{2}".format(_orc_table_name,_start_date,_end_date)
    
    #win_shell = "plink -pw M9rgan?? p000505@itubuntu02 "sudo su - hdfs -c 'hive -e \"SELECT act,from_unixtime(unix_timestamp(dt),'\''EEEE'\'') as sd,hour(dt) as sh, COUNT(*) AS count FROM eSignal.asx_orc WHERE sdt BETWEEN '\''2016-12-01'\'' AND '\''2016-12-30'\'' GROUP BY act,from_unixtime(unix_timestamp(dt),'\''EEEE'\''),hour(dt) ORDER BY act,sd,sh;\" > /home/hdfs/eSignal_test_20161231.csv'""
    if os.name=='nt': # Windows
        win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hdfs -c 'hive -e \\\"{0}\\\" > {1}'".format(hiveSql, tsv_file_for_result)
    elif os.name=='posix': # Linux
        win_shell = "sudo su - hdfs -c 'hive -e \"{0}\" > {1}'".format(hiveSql, tsv_file_for_result)

    print(win_shell) # to check what been passed to win_shell    
    print()
    returncode = subprocess.call(win_shell, shell=True)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished
    # tsv to csv : http://stackoverflow.com/questions/22419979/how-do-i-convert-a-tab-separated-values-tsv-file-to-a-comma-separated-values    
    ubt.ubuntu_move_file(tsv_file_for_result, "/home/{0}/HiveStat/daily_hourly_act_count/".format(login_user))
    ubt.ubuntu_convert_tsv_file_to_csv_file(csv_file_for_result.replace('csv', 'tsv'), csv_file_for_result)
    ubt.ubuntu_rm_a_file(csv_file_for_result.replace('csv', 'tsv'))

    if os.name=='nt':
        win_folder = 'G:\\HiveStat\\daily_hourly_act_count\\'
        ubt.file_transfer_linux_local_to_win_remote(csv_file_for_result, win_folder)
    elif os.name=='posix':
        win_pc_ip = '192.168.0.6'
        share_folder = 'HiveStat'
        linux_from_file = csv_file_for_result
        win_to_flie = 'daily_hourly_act_count\daily_hourly_{0}_{1}_{2}.csv'.format(_orc_table_name,_start_date,_end_date)
        ubt.file_transfer_linux_local_to_win_remote('', '', win_pc_ip, share_folder, linux_from_file, win_to_flie)

#USE database;
