class OsUtility(object):
    """description of class"""

import os
import psutil
import datetime
import IoUtility as iou
import DataTypeUtility as dpu

def create_folder_if_not_exist(_dir):
    if not os.path.exists(_dir):
        os.mkdir(_dir)

def is_process_with_cmdline_exist(_cmdline):

    # @process details, @プロセス情報
    found = False
    for proc in psutil.process_iter():  # https://pythonhosted.org/psutil/
    
        try:
            pid = proc.pid                
            if psutil.Process(pid).cmdline():
                # if list if not empty  ->  # catch process with the specified cmd name regardless it's launched by a main thread or human click
                # ['D:\\PythonApplication2\\PythonApplication1\\ConsoleApplication2.exe']
                proc_cmdline = psutil.Process(pid).cmdline()            
            #if len(psutil.Process(pid).cmdline()) > 2:  
            #    # if list is size more than 3 ->  # only catch process launched by a main thread or through cmd.exe
            #    # ['C:\\Windows\\system32\\cmd.exe', '/c', 'D:\\PythonApplication2\\PythonApplication1\\ConsoleApplication2.exe']                
            #    proc_cmdline = psutil.Process(pid).cmdline()[2]
            else: # システム割込み pid:0, Windows log-on process pid:96
                continue
            process_creation_time = datetime.datetime.fromtimestamp(psutil.Process(pid).create_time()).strftime("%Y-%m-%d %H:%M:%S")
        except:
            continue   
    
        #if 'データ加工_{0}.bat'.format(_exchange) in proc_cmdline:
        for cmd in proc_cmdline:

            if _cmdline in cmd:

                found = True
                #print(proc_cmdline)
                #print(cmd)
                print('There is a existing process for this same task already!')
                return True

    return False

def find_largest_file_in_dir(_dir, _ext, _contained_str=''):
    biggest = ('', -1)
    for f in iou.getfiles(_dir, _ext, _contained_str):
        itemsize = get_file_size(_dir + "/" + f)
        if itemsize > biggest[1]:
                biggest = (f, itemsize)
    return biggest[0]

def find_latest_file_in_dir(_dir, _ext, _contained_str=''):
    latest = ('', dpu.str_to_datetime('1900-01-01 00:00:00'))
    for f in iou.getfiles(_dir, _ext, _contained_str):
        timestamp = get_file_lastupdate(_dir + "/" + f)
        if timestamp > latest[1]:
                latest = (f, timestamp)
    return latest[0]

def get_dirpath_from_filepath(_filepath):
    # http://stackoverflow.com/questions/10149263/extract-a-part-of-the-filepath-a-directory-in-python
    _filepath = _filepath.replace('\\\\','/').replace('\\','/')
    return os.path.dirname(_filepath)

def delete_file(_file):
    if os.path.isfile(_file):
        os.remove(_file)

def rename_file(_old_name, _new_name):
    if os.path.isfile(_new_name):
        os.remove(_new_name)
    os.rename(_old_name, _new_name)

def get_user():    
    return os.environ.get("USERNAME")

def get_current_path(_file):
    return os.path.abspath(_file)  # スクリプトの絶対パス

def get_file_size(_file):
    # http://stackoverflow.com/questions/2104080/how-to-check-file-size-in-python
    try:
        return os.stat(_file).st_size
    except:
        return 0    # the file might compressed and doesn't exist any more.

# File最終更新日時
def get_file_lastupdate(_file):
	# http://stackoverflow.com/questions/375154/how-do-i-get-the-time-a-file-was-last-modified-in-python
	# http://stackoverflow.com/questions/39359245/from-stat-st-mtime-to-datetime
	import datetime
	mod_timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(_file))
	return mod_timestamp

def copy_file(_origin, _dest):
	# http://stackoverflow.com/questions/123198/how-do-i-copy-a-file-in-python
	#-------------------------------------------------------------------------
	#| Function          |Copies Metadata|Copies Permissions|Can Specify Buffer|
	#-------------------------------------------------------------------------
	#| shutil.copy       |      No       |        Yes       |        No        |
	#-------------------------------------------------------------------------
	#| shutil.copyfile   |      No       |         No       |        No        |
	#-------------------------------------------------------------------------
	#| shutil.copy2      |     Yes       |        Yes       |        No        |
	#-------------------------------------------------------------------------
	#| shutil.copyfileobj|      No       |         No       |       Yes        |
	#-------------------------------------------------------------------------
	from shutil import copy
	copy(_origin, _dest)

def find_a_dir(_dir, _search_root):
	# http://stackoverflow.com/questions/1724693/find-a-file-in-python    
    for root, dirs, files in os.walk(_search_root):
        if _dir in files:
            return os.path.join(root, _dir)
