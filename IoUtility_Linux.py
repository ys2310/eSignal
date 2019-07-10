

import os
import codecs
import pandas as pd

import StringUtility as str_util
import GeneralUtility as gn_util
import OsUtility as osu
import DataCollectionUtility as dc_util
import DataTypeUtility as tp_util

def read_all_lines(_file):
    with open(_file) as f:
        return f.readlines()    # 1行毎にファイル終端まで全て読む(改行文字も含まれる)    

def write_to_log(_log_path, _contents, _is_append=False):
    dir_to_file = osu.get_dirpath_from_filepath(_log_path)
    create_folder_if_not_exist(dir_to_file)

def write_to_json(_file, _json_contents, _is_append=False):
    dir_to_file = osu.get_dirpath_from_filepath(_file)
    create_folder_if_not_exist(dir_to_file)
    
    with codecs.open(_file, "a" if _is_append else "w",'utf-8') as f:
        for s in _json_contents:
            f.write("{0}\n".format(s))
    
def move_file(_oldpath,_newpath):
    os.rename(_oldpath, _newpath)

def isReadOnly(_file):
    # ref : http://stackoverflow.com/questions/2113427/determining-whether-a-directory-is-writeable    
    return (not os.access(_file, os.W_OK))

def setReadOnly(_file):
    # ref : http://stackoverflow.com/questions/28492685/change-file-to-read-only-mode-in-python    
    from stat import S_IREAD, S_IRGRP, S_IROTH
    os.chmod(_file, S_IREAD|S_IRGRP|S_IROTH)

def removeReadOnly(_file):
    # ref : http://stackoverflow.com/questions/28492685/change-file-to-read-only-mode-in-python    
    from stat import S_IREAD,S_IWUSR # Need to add this import to the ones above
    os.chmod(_file, S_IWUSR|S_IREAD) # This makes the file read/write for the owner

def get_writable_files(_path, _ext=''):
    from os import listdir
    from os.path import isfile, join    
    onlyfiles = [f for f in listdir(_path) if (isfile(join(_path, f)) and (not isReadOnly(join(_path, f))))]
    if _ext!='':
        onlyfiles = [f for f in onlyfiles if f.endswith(_ext)]   # ref : http://stackoverflow.com/questions/3964681/find-all-files-in-directory-with-extension-txt-in-python
    return onlyfiles

def create_folder_if_not_exist(_directory):    
    if not os.path.exists(_directory):
        os.makedirs(_directory)

def file_exist(_fname):
    import os.path
    return os.path.isfile(_fname)

def delete_file(_outfile):    
    os.remove(_outfile) if file_exist(_outfile) else None

def save_string_as_html(_html_str, _filename):
    with codecs.open(_filename,"w",'utf-8') as Html_file:
        Html_file.write(_html_str)

def write_csv(_list_data_or_dataframe_data, _filepath, _isAppend=False, _row_num=1, _col_num=1, _isHeader=True, _cols_list=None, _isIndex=False, _index_list=None, _sep=','):    
    
    dir_to_file = osu.get_dirpath_from_filepath(_filepath)
    create_folder_if_not_exist(dir_to_file)
    # if not a dataframe, convert the list into 2D array
    if isinstance(_list_data_or_dataframe_data, list):
        _list_data_or_dataframe_data.sort()
        numpy_arr = tp_util.list_to_2D_array(_list_data_or_dataframe_data, _row_num, _col_num)  #numpy.reshape(_list_data, (_row_num, _col_num))
        df = pd.DataFrame(numpy_arr, index=_index_list, columns=_cols_list)                     #pd.DataFrame(_data, columns=_cols_list)
    elif isinstance(_list_data_or_dataframe_data, pd.DataFrame) or isinstance(_list_data_or_dataframe_data, pd.core.series.Series):
        df = _list_data_or_dataframe_data
    elif isinstance(_list_data_or_dataframe_data, str):
        df = pd.DataFrame({'Name':[_list_data_or_dataframe_data]})
    else:
        print('unknown type {0}'.format(type(_list_data_or_dataframe_data)))
        return
    if _isAppend:
        df.to_csv( _filepath, mode='a', sep=_sep, index=_isIndex, header = _isHeader )
    else:
        df.to_csv( _filepath, mode='w', sep=_sep, index=_isIndex, header = _isHeader )

def read_files_in_folders_and_dosomething(_base_dir, _folders, _csv_func, _do_func_list=None):
    # loop through all folders
    for folder in _folders:

        # console title update
        console_title(folder)
        # get all file names
        files = getfiles(_base_dir + folder)
        files = str_util.list_not_contains_str(files, ['$'])

        # clear console
        console_clear() 

        # loop through all files in folder
        for file in files:

            # print current file name
            print(file)
            # file full path
            file = _base_dir + folder + '/' + file
            # column number
            num_cols = get_column_number(file)

            func = lambda : _csv_func(file, num_cols)
            data = gn_util.error_handling(func)
            if(data is None):
                continue
            
            #print(mc.dtypes)
            # can I make the following two funcs passed by parameters?
            print(len(data), " lines")
            #map(compare_time, data['Date'], data['Date'][1:])

            #print(*locals())
            #print(*locals()['_do_func_list'])
            #print(type(*locals()['_do_func_list']))
            #print(*locals()['_do_func_list'][0])
            #param = (locals()['_do_func_list'])
            #print(param[0][1])            

            # do a series of func() in the given order
            for param_tuple in _do_func_list:
                #print(*locals())
                #print(*args in do_func)
                #args = locals()[do_func]
                do_func = param_tuple[0]
                #tmp = list(param_tuple[1:])
                #tmp[0] = data           # 計算結果を使う
                #args = tuple(tmp)
                args = dc_util.setTupleValue(param_tuple[1:], 0, data)
                do_func(*args)  # *を付けると、タプルの各要素を関数の引数として展開されて渡される                

def getfiles(_path, _ext='', _contained_str=''):
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [f for f in listdir(_path) if isfile(join(_path, f))]
    if _ext!='':
        onlyfiles = [f for f in onlyfiles if f.endswith(_ext)]   # ref : http://stackoverflow.com/questions/3964681/find-all-files-in-directory-with-extension-txt-in-python
    if _contained_str!='':
        onlyfiles = [f for f in onlyfiles if _contained_str in f]   # ref : http://stackoverflow.com/questions/3964681/find-all-files-in-directory-with-extension-txt-in-python
    return onlyfiles

def getfolders(_path):
    from os import listdir
    from os.path import isdir, join
    onlyfolders = [f for f in listdir(_path) if isdir(join(_path, f))]
    return onlyfolders

def get_column_number(_file):
    import csv
    with open(_file) as f:
        reader = csv.reader(f, delimiter=',', skipinitialspace=True)
        first_row = next(reader)
        num_cols = len(first_row)
    return num_cols

#def console_title(_text, _encode='utf-8'):
#    import ctypes
    # If you want to use a variable you have to convert it to a bytesobject:
#    if _encode=='shift-jis':
#        ctypes.windll.kernel32.SetConsoleTitleA(_text.encode('shift-jis'))   # utf-8 default encoded
#    else:
#        ctypes.windll.kernel32.SetConsoleTitleA(_text.encode())   # utf-8 default encoded

def console_clear():    
    os.system('cls')

def read_csv(_csv_name, _column_names):
    df = pd.read_csv(_csv_name)
    df.columns = _column_names
    return df
