# -*- coding:shift-jis -*-

import sys
#sys.path.append('../x64/Debug/')
#sys.path.append('../PythonUtility/')

import datetime as dt
import pandas as pd
import numpy as np
#import pyodbc

import IoUtility as iou
#import holidays

# import文を関数内で定義すると、multi-thread 呼び出しの際にうまくいかない場合がある。 2017.11.08
import workalendar as wk
from workalendar import europe, america, usa, canada, oceania, asia, africa
# http://stackoverflow.com/questions/1029794/holiday-files-for-g20-countries

def is_a_workday(_nation, _date):
    cal_dict = {
                # = Europe =
                'Belgium':wk.europe.Belgium(),
                'Czech':wk.europe.CzechRepublic(),
                'Denmark':wk.europe.Denmark(),
                'Estonia':wk.europe.Estonia(),
                'European Central Bank':wk.europe.EuropeanCentralBank(),
                'Finland':wk.europe.Finland(),
                'France':wk.europe.France(),
                #'France (Alsace / Moselle)':wk.europe.France(),
                'Germany':wk.europe.Germany(),
                'Greece':wk.europe.Greece(),
                'Hungary':wk.europe.Hungary(),
                'Iceland':wk.europe.Iceland(),
                'Italy':wk.europe.Italy(),
                'Luxembourg':wk.europe.Luxembourg(),
                'Netherlands':wk.europe.Netherlands(),
                'Norway':wk.europe.Norway(),
                'Poland':wk.europe.Poland(),
                'Portugal':wk.europe.Portugal(),
                'Slovakia':wk.europe.Slovakia(),
                'Sweden':wk.europe.Sweden(),
                'United Kingdom':wk.europe.UnitedKingdom(),
                'UK':wk.europe.UnitedKingdom(),
                'U.K.':wk.europe.UnitedKingdom(),
                'Spain':wk.europe.Spain(),
                'Slovenia':wk.europe.Slovenia(),
                'Switzerland':wk.europe.Switzerland(),
                # = America =
                'Brazil':wk.america.Brazil(),
                'Chile':wk.america.Chile(),
                'Colombia':wk.america.Colombia(),
                'Mexico':wk.america.Mexico(),
                'Panama':wk.america.Panama(),
                'United States':wk.usa.UnitedStates(),
                'Canada':wk.canada.Canada(),
                # = Asia =
                'Japan':wk.asia.Japan(),
                'Qatar':wk.asia.Qatar(),
                'South Korea':wk.asia.SouthKorea(),
                'Taiwan':wk.asia.Taiwan(),
                #'China':wk.asia.China(),
                # = Oceania =
                'Australia':wk.oceania.Australia(),
                'Marshall Islands':wk.oceania.MarshallIslands(),
                # = Africa =
                'Algeria':wk.africa.Algeria(),
                'Benin':wk.africa.Benin(),
                'Ivory Coast':wk.africa.IvoryCoast(),
                'Madagascar':wk.africa.Madagascar(),
                #'Sﾃ｣o Tomﾃｩ':wk.africa.SaoTome(),
                'South Africa':wk.africa.SouthAfrica(),
               }

    cal = cal_dict[_nation] #wk.europe.France()
    #fr_holidays = cal.holidays(2013)
    #print(fr_holidays)
    #print('')
    #print(cal.is_working_day(dt.date(2013,12,29)))
    return cal.is_working_day(_date)

def is_a_holiday(_nation, _date):
    return not is_a_workday(_nation, _date)

if __name__ == '__main__':
    x = is_a_holiday('Japan',dt.date(2016,8,28))
    print(x)
