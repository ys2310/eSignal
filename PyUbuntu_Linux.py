# -*- coding:shift-jis -*-

import sys
import os
import os.path
import ctypes
import pandas as pd
from datetime import datetime as dt

import subprocess
from subprocess import check_output

def ubuntu_disk_usage(_machine_name):
    
    # http://askubuntu.com/questions/5444/how-to-find-out-how-much-disk-space-is-remaining    
    win_shell = 'plink -pw M9rgan?? p000505{0} df -h'.format(_machine_name)
    linux_shell = 'df -h'.format(_machine_name)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell,'/n') # to check what been passed to win_shell
    result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
                                                                   # need to decode to a string first. : http://stackoverflow.com/questions/2502833/store-output-of-subprocess-popen-call-in-a-string
                                                                   # trim() the end '\n' appended by Linux system : http://stackoverflow.com/questions/15374211/why-does-popen-communicate-return-bhi-n-instead-of-hi
    #returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

    return result    

# given a directory, get the count of files in that directory.
def ubuntu_count_num_of_files_in_directory(_dir_path):
   
    win_shell = 'plink -pw M9rgan?? p000505@itubuntu02 find {0} -type f | wc -l'.format(_dir_path)    
    linux_shell = 'find {0} -type f | wc -l'.format(_dir_path)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell,'/n') # to check what been passed to win_shell    
    result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
                                                                   # need to decode to a string first. : http://stackoverflow.com/questions/2502833/store-output-of-subprocess-popen-call-in-a-string
                                                                   # trim() the end '\n' appended by Linux system : http://stackoverflow.com/questions/15374211/why-does-popen-communicate-return-bhi-n-instead-of-hi
    #returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

    return result

def ubuntu_find_files_with_certain_text(_search_path, _search_text):

    # https://stackoverflow.com/questions/16956810/how-do-i-find-all-files-containing-specific-text-on-linux
    linux_shell = "grep -Ril '{1}' '{0}'".format(_search_path, _search_text)
    print(linux_shell)
    result = subprocess.run(linux_shell, shell=True, check=True)
    return result

def ubuntu_find_dir_with_certain_name(_search_str):
    # http://askubuntu.com/questions/123305/how-to-find-a-folder-on-my-server-with-a-certain-name
    # C:\Users\steve> plink -pw M9rgan?? p000505@itubuntu02 "find / -xdev 2>/dev/null -name "TSE""    
    win_shell = 'plink -pw M9rgan?? p000505@itubuntu02 find / -xdev 2>/dev/null -name \\\"{0}\\\"'.format(_search_str)
    linux_shell = 'find / -xdev 2>/dev/null -name \\\"{0}\\\"'.format(_search_str)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell,'/n') # to check what been passed to win_shell    
    result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
                                                                   # need to decode to a string first. : http://stackoverflow.com/questions/2502833/store-output-of-subprocess-popen-call-in-a-string
                                                                   # trim() the end '\n' appended by Linux system : http://stackoverflow.com/questions/15374211/why-does-popen-communicate-return-bhi-n-instead-of-hi
    #returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

    return result

def ubuntu_get_file_list(_path_to_dir, _exculde_file_expr=''):

    if _exculde_file_expr != '':
        # http://askubuntu.com/questions/512961/exclude-certain-files-in-ls        
        win_shell = 'plink -pw M9rgan?? p000505@itubuntu02 ls {0} -I {1}'.format(_path_to_dir, _exculde_file_expr)        
        linux_shell = 'ls {0} -I {1}'.format(_path_to_dir, _exculde_file_expr)
    else:        
        win_shell = 'plink -pw M9rgan?? p000505@itubuntu02 ls {0}'.format(_path_to_dir)        
        linux_shell = 'ls {0}'.format(_path_to_dir)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell,'/n') # to check what been passed to win_shell    
    result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
    return result.split('\n')

def ubuntu_move_file(_path_to_old_file, _path_to_new_file):

    print('move old file to new file : {0} ⇒ {1}'.format(_path_to_old_file, _path_to_new_file))
    
    win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo mv {0} {1}".format(_path_to_old_file, _path_to_new_file)
    linux_shell = "sudo mv {0} {1}".format(_path_to_old_file, _path_to_new_file)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell) # to check what been passed to win_shell
    print()
    if os.path.exists(_path_to_old_file):
        result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
    else:
        result = False
    return result

def ubuntu_convert_tsv_file_to_csv_file(_path_to_tsv_file, _path_to_csv_file):

    print('converting tsv to csv : {0}'.format(_path_to_tsv_file))    
    win_shell = "plink -pw M9rgan?? p000505@itubuntu02 \"tr '\t' ',' < {0} > {1}\"".format(_path_to_tsv_file, _path_to_csv_file)    
    linux_shell = "tr '\t' ',' < {0} > {1}".format(_path_to_tsv_file, _path_to_csv_file)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell) # to check what been passed to win_shell
    print()
    if os.path.exists(_path_to_tsv_file):
        returncode = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html                                                         # returns a byte object. 
    else:
        returncode = False
    return returncode

def ubuntu_convert_sjis_file_to_utf8_file(_path_to_sjis_file, _path_to_utf8_file):

    print('converting sjis to utf8 : {0}'.format(_path_to_sjis_file))
    
    win_shell = 'plink -pw M9rgan?? p000505@itubuntu02 iconv -f sjis -t utf8 {0} > {1}'.format(_path_to_sjis_file, _path_to_utf8_file)    
    linux_shell = 'iconv -f sjis -t utf8 {0} > {1}'.format(_path_to_sjis_file, _path_to_utf8_file)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(linux_shell,'/n') # to check what been passed to win_shell
    if os.path.exists(_path_to_sjis_file):
        result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
    else:
        result = False
    return result

def ubuntu_rm_a_file(_path_to_file):

    win_shell = 'plink -pw M9rgan?? p000505@itubuntu02 rm -f {0}'.format(_path_to_file)    
    linux_shell = 'rm -f {0}'.format(_path_to_file)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell,'/n') # to check what been passed to win_shell    
    if os.path.exists(_path_to_file):
        result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
    else:
        result = False
    return result

def ubuntu_rm_all_files(_path_to_dir):

    # without concerning the "Argument too long error"
    # https://serverlog.jp/rm-argument-list-too-long/
    # find /path/to/dir -type f -print | xargs rm
    
    win_shell = 'plink -pw M9rgan?? p000505@itubuntu02 find {0} -type f -print | xargs rm'.format(_path_to_dir)    
    linux_shell = 'find {0} -type f -print | xargs rm'.format(_path_to_dir)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell,'/n') # to check what been passed to win_shell
    result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
    return result

# split a large file into small subfiles
def ubuntu_split_file_into_subs(_path_to_file, _subfile_prefix_with_destination_dir_path, _split_lines=100000):
    # --verbose : displaying the progress details
    # -d : subfiles are named with continuous numeric numbers
    # -l : split every n lines into a sub flie
    # the new subfile prefix can contains the destination directory path 
    # ex.> split --verbose -d -l 100000 /home/hdfs/test_0_2016-09-05.csv /home/p000505/test_0_2016-12-16/    
    win_shell = 'plink -pw M9rgan?? p000505@itubuntu02 split --verbose -d -l {2} {0} {1}'.format(_path_to_file, _subfile_prefix_with_destination_dir_path, _split_lines)    
    linux_shell = 'split --verbose -d -l {2} {0} {1}'.format(_path_to_file, _subfile_prefix_with_destination_dir_path, _split_lines)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell,'/n') # to check what been passed to win_shell    
    result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object. 
                                                                   # need to decode to a string first. : http://stackoverflow.com/questions/2502833/store-output-of-subprocess-popen-call-in-a-string
                                                                   # trim() the end '\n' appended by Linux system : http://stackoverflow.com/questions/15374211/why-does-popen-communicate-return-bhi-n-instead-of-hi
    #returncode = subprocess.call(cmd)   # start and block until done : http://stackoverflow.com/questions/2837214/python-popen-command-wait-until-the-command-is-finished

    return result

# transfer win local file onto linux local directory with the login user permission
def file_transfer_win_local_to_linux_remote(_win_file_path, _linux_destination_directory_path):
    
    ctypes.windll.kernel32.SetConsoleTitleW("Win ⇒ Linux ファイル転送中...")
   
    files_num_before = ubuntu_count_num_of_files_in_directory(_linux_destination_directory_path)

    # correct the windows path format so that WinScp recognizes it
    _win_file_path = _win_file_path.replace('\\','\\\\')    # replace single backslash with double backslash \ ⇒ \\
    _win_file_path = _win_file_path.replace('/','\\\\')
    # correct the linux path format so that WinScp recognizes it
    _linux_destination_directory_path = _linux_destination_directory_path.replace('\\\\', '\\')
    _linux_destination_directory_path = _linux_destination_directory_path.replace('\\', '/')
    
    #win_shell = 'winscp /command "option batch on" "option confirm off" "open p000505:M9rgan??@itubuntu02" "put ""C:\\Users\\P000505\\Desktop\\parameter.csv"" "/home/p000505/local/"' # replace single backslash with double backslash \ ⇒ \\
    win_shell = 'winscp /command "option batch on" "option confirm off" "open p000505:M9rgan??@itubuntu02" "put ""{0}"" {1}" "exit"'.format(_win_file_path, _linux_destination_directory_path)
    linux_shell = ""

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell,'\n') # to check what been passed to win_shell
    result = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html                                                       
    
    files_num_after = ubuntu_count_num_of_files_in_directory(_linux_destination_directory_path)

    number_of_files_transfered = int(files_num_after) - int(files_num_before)
    print('Total number of files transfered to the destination directory : {0}'.format(number_of_files_transfered))
    print()
    return number_of_files_transfered


def dir_transfer_linux_local_to_win_remote(_linux_file_path='', _win_destination_dir_path='', _win_pc_ip='', _win_pc_share_folder='', _linux_from_file='', _win_to_file='', _display_stdout_or_not=True):
    
    if os.name=='nt':
        ctypes.windll.kernel32.SetConsoleTitleW("Linux ⇒ Windows ファイル転送中...")

    # correct the windows path format so that WinScp recognizes it
    #_win_file_path = _win_file_path.replace('\\','\\\\')    # replace single backslash with double backslash \ ⇒ \\
    _win_destination_dir_path = _win_destination_dir_path.replace('/','\\')
    # correct the linux path format so that WinScp recognizes it
    #_linux_destination_directory_path = _linux_destination_directory_path.replace('\\\\', '\\')
    #_linux_destination_directory_path = _linux_destination_directory_path.replace('\\', '/')

    # win shell    
    win_shell = ''
    # linux shell
    # smbclient usage : http://www.samba.gr.jp/project/translation/3.5/htmldocs/manpages-3/smbclient.1.html
    # -c option : http://www.samba.gr.jp/project/translation/3.5/htmldocs/manpages-3/smbclient.1.html
    # mput : http://blog.jojo.jp/?eid=1424821
    # original : smbclient '\\192.168.0.6\QCollector Expert For eSignal' -U steve%M9rgan?? -c "put /home/p000505/HiveStat/youbi_hourly_act_count/youbi_hourly_eSignal.SHG_orc_2017-02-23_2017-02-23.csv test_youbi_hourly_2017.csv"
    linux_shell = "smbclient '\\\\{0}\\{1}' -U steve%M9rgan?? -c \"mput \\\"{2}\\\" \\\"{3}\\\" \" ".format(_win_pc_ip, _win_pc_share_folder, _linux_from_dir, _win_to_dir)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    if _display_stdout_or_not==True:
        print(shell) # to check what been passed to win_shell
        print()
    #result = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html
    if os.path.exists(_linux_from_dir):
        result = check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object.
    else:
        result = False

def file_transfer_win_remote_to_linux_local(_win_pc_ip, _win_pc_share_folder, _win_from_sub_folder, _win_from_file, _linux_to_dir):

    cmd = "smbclient '\\\\{0}\\{1}' -c \"lcd {4}; cd {2}; get {3}; exit; \" -U steve%M9rgan??".format(_win_pc_ip, _win_pc_share_folder, _win_from_sub_folder, _win_from_file, _linux_to_dir)
#    os.system(cmd)
    print(cmd)
    result = subprocess.run(cmd, shell=True, check=True)
    return result

def file_transfer_linux_local_to_win_remote(_linux_file_path, _win_destination_dir_path, _win_pc_ip, _win_pc_share_folder, _linux_from_file, _win_to_file):
    
    if os.name=='nt':
        ctypes.windll.kernel32.SetConsoleTitleW("Linux ⇒ Windows ファイル転送中...")

    # correct the windows path format so that WinScp recognizes it
    #_win_file_path = _win_file_path.replace('\\','\\\\')    # replace single backslash with double backslash \ ⇒ \\
    _win_destination_dir_path = _win_destination_dir_path.replace('/','\\')
    # correct the linux path format so that WinScp recognizes it
    #_linux_destination_directory_path = _linux_destination_directory_path.replace('\\\\', '\\')
    #_linux_destination_directory_path = _linux_destination_directory_path.replace('\\', '/')

    # win shell    
    win_shell = 'winscp /command "option batch on" "option confirm off" "open p000505:M9rgan??@itubuntu02" "get -delete {0} {1}" "exit"'.format(_linux_file_path, _win_destination_dir_path)
    # linux shell
    # smbclient usage : http://www.samba.gr.jp/project/translation/3.5/htmldocs/manpages-3/smbclient.1.html
    # -c option : http://www.samba.gr.jp/project/translation/3.5/htmldocs/manpages-3/smbclient.1.html
    # original : smbclient '\\192.168.0.6\QCollector Expert For eSignal' -U steve%M9rgan?? -c "put /home/p000505/HiveStat/youbi_hourly_act_count/youbi_hourly_eSignal.SHG_orc_2017-02-23_2017-02-23.csv test_youbi_hourly_2017.csv"
    linux_shell = "smbclient '\\\\{0}\\{1}' -U steve%M9rgan?? -c \"put \\\"{2}\\\" \\\"{3}\\\" \" ".format(_win_pc_ip, _win_pc_share_folder, _linux_from_file, _win_to_file)

    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell) # to check what been passed to win_shell
    print()
    #result = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html
    if os.path.exists(_linux_from_file):
        result = subprocess.run(shell, shell=True, check=True) #check_output(shell.split(' ')).decode('utf-8').strip()  # returns a byte object.
    else:
        result = False

def ubuntu_split_file_into_chunks(_input_file, _prefix, _num_of_splits=20):

    win_shell = ""
    linux_shell = "split --additional-suffix=.csv -d -n l/{0} {1} {2}".format(_num_of_splits, _input_file, _prefix)
    
    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell) # to check what been passed to win_shell
    print()
    result = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html

def ubuntu_delete_all_files_from_a_directory(_dir_path):
    k

def ubuntu_unzip_7z_files(_compressed_tar_zip, _unzip_dir_path):
    # http://askubuntu.com/questions/341628/how-to-extract-tar-7z-files-from-command-line
    #> sudo apt-get install p7zip-full    
    win_shell = 'winscp /command "option batch on" "option confirm off" "open p000505:M9rgan??@itubuntu02" "w7z x -so {0}.tar.7z | tar xf - -C {1}" "exit"'.format(_compressed_tar_zip, _unzip_dir_path)
    linux_shell = ""
    
    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell) # to check what been passed to win_shell
    print()
    result = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html

def ubuntu_transfer_file_linux_to_linux(_from_file, _to_file, _user, _pc_hostname):
    # Transfer files between Linux machines
    # http://superuser.com/questions/345347/how-to-copy-a-file-between-two-linux-machines    
    win_shell = 'winscp /command "option batch on" "option confirm off" "open p000505:M9rgan??@itubuntu02" "scp {0} ssh://[{2}]@[{3}]/[{1}]" "exit"'.format(_from_file, _to_file, _user, _pc_hostname)
    linux_shell = ""
    
    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell
    
    print(shell) # to check what been passed to win_shell
    print()
    result = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html

def ubuntu_empty_trashcan():
    # Ubuntu trashcan 削除
    # http://askubuntu.com/questions/468721/how-can-i-empty-the-trash-using-terminal
    win_shell = 'winscp /command "option batch on" "option confirm off" "open p000505:M9rgan??@itubuntu02" "rm -rf ~/.local/share/Trash/*" "exit"'
    linux_shell = ""
    
    if os.name=='nt':
        shell = win_shell
    elif os.name=='posix':
        shell = linux_shell

    print(shell) # to check what been passed to win_shell
    print()
    result = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html    
    # or using a package tool, you can type the following commands
    # > sudo apt-get install trash-cli
    # > trash-empty

def ubuntu_show_package_version(_package):
    # To investigate a package's version    
    win_shell = 'winscp /command "option batch on" "option confirm off" "open p000505:M9rgan??@itubuntu02" "pip show {0}" "exit"'.format(_package)
    print(shell) # to check what been passed to win_shell
    print()
    result = subprocess.run(shell, shell=True, check=True)    # stdout : http://docs.python.jp/3/library/subprocess.html    

def ubuntu_enabling_networking():
    # http://askubuntu.com/questions/207020/command-for-enabling-networking
    # sudo service networkng start
    w
