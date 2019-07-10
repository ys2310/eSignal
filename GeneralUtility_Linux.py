def multi_proc(func):
    from multiprocessing import Pool
    from multiprocessing import Process
    p = Pool(10)    # 最大プロセス数:10
    p.map(func, range(8))

def measure_clock(_func, _digit=3):
    import time
    import FormatUtility as fmt_util
    start = time.time()
    res = _func()
    end = time.time()
    print(_func.__name__ + " takes ", fmt_util.format_decimal(end - start, _digit), " sec")
    return 

def error_handling(func, _optional_err_msg=""):
    try:
        return func()
        #if num_cols == 13:
        #    #mc = py_util.measure_clock(lambda : pd.read_csv(file, encoding='Shift-JIS', header=None))
        #    mc = py_util.measure_clock(lambda : pd.read_csv(file, encoding='Shift-JIS', header=None, names=('Date','Time','TP','TQ','TE','Dummy','BP','BE','BQ','AP','AE','AQ','BxA'), dtype={'Date':object, 'Time':object ,'TP':float, 'TQ':float, 'TE':str, 'Dummy':str, 'BP':float, 'BE':str, 'BQ':float, 'AP':float, 'AE':str, 'AQ':float, 'BxA':str}))            
        #else:
        #    mc = py_util.measure_clock(lambda : pd.read_csv(file, encoding='Shift-JIS', header=None, names=('Date','Time','TP','TQ','TE','Dummy','BP','BE','BQ','AP','AE','AQ'), dtype={'Date':object, 'Time':object ,'TP':float, 'TQ':float, 'TE':str, 'Dummy':str, 'BP':float, 'BE':str, 'BQ':float, 'AP':float, 'AE':str, 'AQ':float}))
    except Exception(e):
        #print("read_csv error @ " + file)
        if(_optional_err_msg):
            print(_optional_err_msg)
        print("error msg : " + str(e))
        return None
        #continue