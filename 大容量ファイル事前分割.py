# -*- coding:shift-jis -*-
import getpass
user = getpass.getuser()

import os
currentWorkingDirectory = "C:\\Users\\{0}\\Desktop\\PySong2\\eSignal".format(user)
os.chdir(currentWorkingDirectory)

import sys
sys.path.append('../PythonStats/x64/Debug/')
sys.path.append('../PyUtility/')

import OsUtility as osu
import IoUtility as iou
import WebUtility as wbu

import sys
import os.path
import pandas as pd
from datetime import datetime as dt

#def bin_div(f, size=316384*16384):
#    l = os.path.getsize(f)
#    div_num = (l + size - 1) / size
#    last = (size * div_num) - l
    
#    b = open(f, 'rb')
#    for i in range(div_num):
#    	read_size = last if i == div_num-1 else size
#    	data = b.read(read_size)
#    	out = open(f + '.frac' + str(i), 'wb')
#    	out.write(data)
#    	out.close()
#    b.close()
    
if __name__=='__main__':
	#if len(sys.argv) > 1:

    print("引数の総個数 = {0}\n".format(len(sys.argv)))
    if len(sys.argv) != 2:
        print("引数を正しく指定してください！")

    for i,x in enumerate(sys.argv):
        print("{0}番目の引数 = {1}\n".format(i, x))

    minimum_devide_to_line_num = 50000

    infolder = 'G:/QCollector Expert For eSignal/{0}/'.format(sys.argv[1])

    #iou.removeReadOnly(infolder + 'DX #F_0.csv')
    #os.remove(infolder + 'DX #F_0.csv')

    for infile in iou.getfiles(infolder, '_0.csv'):

        infile = infolder + infile

        # process only files larger than 700 MB
        if osu.get_file_size(infile) < 700000000:
            continue

        print('{0} を分割処理中...'.format(infile))

        last_line = ""
        lines = []
        counter = 0
        frac_num = 1
        # http://stackoverflow.com/questions/8009882/how-to-read-large-file-line-by-line-in-python
        with open(infile) as fin:
            for line in fin:            
                #print(counter,' : ',line)
                this_timestamp = line.split(',')[0] + ' ' + line.split(',')[1]
                if counter > minimum_devide_to_line_num and dt.strptime(this_timestamp, '%Y-%m-%d %H:%M:%S').hour >= 7 and \
                    (
                     dt.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S').hour < 7 \
                     #         Last timestamp        This timestamp
                     # ex.1 〇 2016/09/13 06:00:00 | 2016/09/13 07:00:00
                     or \
                     (dt.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S').hour > dt.strptime(this_timestamp, '%Y-%m-%d %H:%M:%S').hour)
                     #         Last timestamp        This timestamp
                     # ex.1 X  2016/09/12 08:00:00 | 2016/09/13 04:09:05
                     # ex.2 X  2016/09/12 23:59:59 | 2016/09/13 00:00:01
                     # ex.3 〇 2016/09/12 23:59:59 | 2016/09/13 07:00:01
                    ):
                    
                    outfile = infile.replace('_0.csv', '_frac{0}_0.csv'.format(frac_num))
                    minor_num = 0    
                    while iou.file_exist(outfile):
                        outfile = infile.replace('_0.csv', '_frac{0}({1})_0.csv'.format(frac_num, minor_num))
                        minor_num = minor_num + 1
                    with open(outfile, 'w') as fout:
                        fout.writelines(lines)
                        frac_num = frac_num + 1
                    lines = []
                    counter = 0            
                lines.append(line)
                counter = counter + 1
                last_timestamp = line.split(',')[0] + ' ' + line.split(',')[1]

        iou.removeReadOnly(infile)
        os.remove(infile)