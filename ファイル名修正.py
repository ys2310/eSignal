# -*- coding:shift-jis -*-
import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')

import IoUtility as iou
import OsUtility as osu
import DatetimeUtility as dmu

import time
from subprocess import call

import pandas as pd

# 間違って2016-08-032016-08-04.csvのようになったファイル名_Postfixを修正
if __name__ == "__main__":

    portfolio = 'TSE'
    for file in iou.getfiles('F:/QCollector Expert For eSignal/{0}'.format(portfolio), '.csv'):
        file = 'F:/QCollector Expert For eSignal/' + portfolio + '/' + file
        newfile = file.split('_0_')[0] + '_0.csv'
        os.rename(file, newfile)
        try:
            date = pd.read_csv(newfile, header=None).iloc[0].ix[0]
            #iou.setReadOnly(file)
            os.rename(newfile, newfile.replace('.csv','_{0}.csv'.format(date.replace('/','-'))))
        except:
            print('skipped {0}'.format(newfile))
        #file.replace('.csv','_{0}.csv'.format(date.replace('/','-'))))
