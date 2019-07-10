class evExtraction(object):
    """description of class"""

import sys
#sys.path.append('../x64/Debug/')
sys.path.append('../x64/Release/')
sys.path.append('../PyUtility/')

import time
import datetime
import DbUtility as db_util
import IoUtility as io_util
import DataTypeUtility as tp_util
import DatetimeUtility as dt_util
import FormatUtility as fmt_util
import GeneralUtility as gn_util
import GuiUtility as gui_util
from numba import jit

# BoostPython
import CppStats

mecab64 = CppStats.MeCab64()
mecab64.Start("ジョニーは戦場へ行った。".encode('shift-jis'))

cabocha64 = CppStats.CaboCha64()
cabocha64.Start("米シティグループが世界の為替取引の市場シェアで３年連続トップだったことが、ユーロマネー・インスティテューショナル・インベスター誌の調査で明らかになった。".encode('shift-jis'))

def tsql_bevt_ptr1(_db_name , _schema, _table, _product):
    sqlstr = """
    -- pattern.1
    -- BP↑[--this duration--]TP↓
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]
                        ,[MicroPrice]
					    ,[AutoId]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'TQ.↓' ) OR ([ActType] = 'BP' and Chg > 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE ActType <> PreviousAct and ActType = 'TQ.↓' and Duration >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_bevt_ptr2(_db_name , _schema, _table, _product):
    sqlstr = """
    -- pattern.2
    -- TP↓BP↓[--this duration--]BP↑
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg,
			    LAG(ActType,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'TP.↓' ) OR ([ActType] = 'BP')) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'TP.↓' AND (PreviousAct = 'BP' and PreviousChg < 0) AND (ActType = 'BP' and Chg > 0) AND Duration  >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_bevt_ptr3(_db_name , _schema, _table, _product):
    sqlstr = """
    -- pattern.3
    -- BP↑[--this duration--]BP↓
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (([ActType] = 'AP' and Chg < 0)) AND Symbol = '{3}' and DateTime < '2016-04-01 00:00:01.000'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE Duration >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_bevt_ptr4(_db_name , _schema, _table, _product):
    sqlstr = """        
    -- pattern.4
    -- TP↓BP↓[--this duration,PriceChg--]TP↑
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration,
	    Value - PreviousValue AS ValChg
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousValue,
			    LAG(ActType,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]                        
					    ,[Value]
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType LIKE 'TP.%' ) OR ([ActType] = 'BP')) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'TP.↓' AND (PreviousAct = 'BP' and PreviousChg < 0) AND ActType = 'TP.↑' AND Duration  >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_bevt_ptr5(_db_name , _schema, _table, _product):
    sqlstr = """    
    -- pattern.5
    -- TP↓TP↓BP↓[--this duration--]AP↓
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT 
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,	
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg	
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]      
					    ,[DateTime]                        
				        ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'AP' and Chg < 0 ) OR ([ActType] = 'BP' and Chg < 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE ActType <> PreviousAct and ActType = 'AP' and Duration  >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_bevt_ptr6(_db_name , _schema, _table, _product):
    sqlstr = """
    -- pattern.6
    -- TP↑TP↑<-TotalQty,
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousValue
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType = 'TQ.↑' ) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE (Value + PreviousValue > 20) and Duration < 20
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_bevt_ptr7(_db_name , _schema, _table, _product):
    sqlstr = """    
    -- pattern.7
    -- BP↓AP↓[--this duration,PriceChg--]AP↑
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration,
	    Value - PreviousValue AS ValChg
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousValue,
			    LAG(ActType,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]                        
					    ,[Value]
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'AP') OR ([ActType] = 'BP' and Chg < 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'BP' AND (PreviousAct = 'AP' and PreviousChg < 0) AND (ActType = 'AP' and Chg > 0) AND Duration  >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_bevt_ptr8(_db_name , _schema, _table, _product):
    sqlstr = """    
    -- pattern.8
    -- TP↓[--this duration--]AP↑
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Value,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousValue,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'TQ.↓' ) OR ([ActType] = 'AP' and Chg > 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PreviousAct = 'TQ.↓' and ActType = 'AP' and Duration > 1
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_aevt_ptr1(_db_name , _schema, _table, _product):
    sqlstr = """
    -- pattern.1
    -- AP↓[--this duration--]TP↑
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'TQ.↑' ) OR ([ActType] = 'AP' and Chg < 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE ActType <> PreviousAct and ActType = 'TQ.↑' and Duration >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_aevt_ptr2(_db_name , _schema, _table, _product):
    sqlstr = """
    -- pattern.2
    -- TP↑AP↑[--this duration--]AP↓
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg,
			    LAG(ActType,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'TP.↑' ) OR ([ActType] = 'AP')) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'TP.↑' AND (PreviousAct = 'AP' and PreviousChg < 0) AND (ActType = 'AP' and Chg > 0) AND Duration  >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_aevt_ptr3(_db_name , _schema, _table, _product):
    sqlstr = """
    -- pattern.3
    -- AP↓[--this duration--]AP↓
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (([ActType] = 'AP' and Chg < 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE Duration >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_aevt_ptr4(_db_name , _schema, _table, _product):
    sqlstr = """        
    -- pattern.4
    -- TP↑AP↑[--this duration,PriceChg--]TP↓
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration,
	    Value - PreviousValue AS ValChg
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousValue,
			    LAG(ActType,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]                        
					    ,[Value]
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType LIKE 'TP.%' ) OR ([ActType] = 'AP')) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'TP.↑' AND (PreviousAct = 'AP' and PreviousChg < 0) AND ActType = 'TP.↓' AND Duration  >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_aevt_ptr5(_db_name , _schema, _table, _product):
    sqlstr = """    
    -- pattern.5
    -- TP↑TP↑AP↑[--this duration--]BP↑
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT 
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,	
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg	
			    FROM
			    (
				    SELECT TOP 1000
					    [Chg]
					    ,[ActType]      
					    ,[DateTime]                        
				        ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'BP' and Chg > 0 ) OR ([ActType] = 'AP' and Chg > 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE ActType <> PreviousAct and ActType = 'BP' and Duration  >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_aevt_ptr6(_db_name , _schema, _table, _product):
    sqlstr = """
    -- pattern.6
    -- TP↓TP↓<-TotalQty,
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousValue
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType = 'TQ.↓' ) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE (Value + PreviousValue > 20) and Duration < 20
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_aevt_ptr7(_db_name , _schema, _table, _product):
    sqlstr = """    
    -- pattern.7
    -- AP↑BP↑[--this duration,PriceChg--]BP↓
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration,
	    Value - PreviousValue AS ValChg
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousValue,
			    LAG(ActType,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY DateTime,AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]                        
					    ,[Value]
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'BP') OR ([ActType] = 'AP' and Chg > 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'AP' AND (PreviousAct = 'BP' and PreviousChg > 0) AND (ActType = 'BP' and Chg < 0) AND Duration  >= 0
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_aevt_ptr8(_db_name , _schema, _table, _product):
    sqlstr = """    
    -- pattern.8
    -- TP↑[--this duration--]BP↓
    SELECT
    *
    FROM
    (
	    SELECT
	    *,
	    DATEDIFF(s, PreviousDatetime, Datetime) AS Duration
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(ActType,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousAct,
			    LAG(Value,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousValue,
			    LAG(Datetime,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY DateTime,AutoId) AS PreviousChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]                        
					    ,[AutoId]
                        ,[MicroPrice]
				    FROM [{0}].[{1}].[{2}]
				    WHERE ((ActType = 'TQ.↑' ) OR ([ActType] = 'BP' and Chg < 0)) AND Symbol = '{3}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PreviousAct = 'TQ.↑' and ActType = 'BP' and Duration > 1
    ORDER BY DateTime
    """.format(_db_name , _schema, _table, _product)
    return sqlstr

def tsql_trades(_db_name, _schema, _table, _product, _evt_time):
    sqlstr = """
    SELECT [DateTime], [ActType], [Value], [MicroPrice]
	FROM [{0}].[{1}].[{2}]
	WHERE ActType LIKE 'TP%' and Symbol = '{3}' and DateTime >= '{4}'
    ORDER BY DateTime
    """.format(_db_name, _schema, _table, _product, _evt_time)
    return sqlstr

def tsql_microprice(_db_name, _schema, _table, _product, _evt_time):
    sqlstr = """
    SELECT [DateTime], [ActType], [Value], [MicroPrice]
	FROM [{0}].[{1}].[{2}]
	WHERE ActType LIKE '%P%' and Symbol = '{3}' and DateTime > '{4}'
    ORDER BY DateTime
    """.format(_db_name, _schema, _table, _product, _evt_time)
    return sqlstr

def tsql_leadtime(_db_name, _schema, _table, _product, _evt_time, _num_of_obs=100):
    sqlstr = """
    SELECT TOP 1 
	LEAD(DateTime,{0}-1,NULL) OVER(ORDER BY DateTime) AS Next100TradeTimestamp 
	FROM [{1}].[{2}].[{3}]
	WHERE ActType LIKE 'TP%' and Symbol = '{4}' and DateTime >= '{5}'
    """.format(_num_of_obs, _db_name, _schema, _table, _product, _evt_time)
    return sqlstr

def tsql_benchprice(_db_name, _schema, _table, _product, _evt_time):    # find the microprice nearest to an evt
    sqlstr = """
    SELECT TOP 1            
    [MicroPrice],[DateTime]
    FROM [{0}].[{1}].[{2}]
    WHERE Symbol = '{3}' and Datetime < '{4}'
    ORDER BY Datetime DESC
    """.format(_db_name, _schema, _table, _product, _evt_time)
    return sqlstr

def tsql_post_evt_stat(_db_name, _schema, _table, _product, _benchprice, _evt_time, _lead_time, _num_of_obs):
    sqlstr = """
    SELECT 
	COUNT(CASE WHEN Value > {4} THEN 1 END) / ({7} * 1.0) AS UpProb,
	COUNT(CASE WHEN Value < {4} THEN 1 END) / ({7} * 1.0) AS DnProb
	FROM [{0}].[{1}].[{2}]
	WHERE Symbol = '{3}' AND (DateTime Between '{5}' and '{6}') AND ActType LIKE 'TP%'
    """.format(_db_name, _schema, _table, _product, _benchprice, _evt_time, _lead_time, _num_of_obs)
    return sqlstr

# do some test
#selected_cols = ["Date", "Time", "ActType"]
##sqlstr = tsql_bevt_ptr1("DWConfiguration","dbo","configuration_node")
#sqlstr = tsql_bevt_ptr1("eSignal_201604","dbo","TOCOM")
##db_cursor = db_util.mssql_exec("P11013", "sa", "Bigdata01", "DWConfiguration", sqlstr)
#db_cursor = db_util.mssql_exec("STEVE-PC", "sa", "Bigdata01", "eSignal_201604", sqlstr)
#db_util.show_rows(db_cursor)

import OsUtility as os_util
server_instance = "P11013" if os_util.get_user() == "operator" else "STEVE-PC"
login_user = "sa"
login_pass = "Bigdata01"
product_list = ["1321-TSE"] if os_util.get_user() == "operator" else ["THU N6-TCM"]
db_list = ["DWConfiguration"] if os_util.get_user() == "operator" else ["eSignal_201603", "eSignal_201602", "eSignal_201601", "eSignal_201512"]
schema = "dbo"
table = "configuration_node" if os_util.get_user() == "operator" else "TOCOM"
#evt_time_list = ["2015-11-27 12:49:34.000", "2015-11-28 12:49:34.000"] if os_util.get_user() == "operator" else ["2016-04-01 01:13:16.000", "2016-04-01 01:13:16.000"]
num_of_obs = 100
interval_in_sec = 300

def calc_all_ptr(ptr):

    import pandas as pd
    c = pd.DataFrame(columns = ['duration','uratio', 'ba', 'product', 'ptr', 'DateTime'])
    
    for product in product_list:
        for db_name in db_list:
            for ba in ['b','a']:
                #for ptr in ptr_list: #[1, 2, 3, 4, 5, 6, 7, 8]:

                # console title
                import ctypes
                osu.set_console_title("{0} {1} {2} {3}".format(db_name, product, ba, ptr))

                print("started {0} {1} {2} {3}".format(db_name, product, ba, ptr))
                # header
                # print("""         EvTime         |    BenchPrc      |      u:d""")
                # evts
                if ba == 'b':
                    if ptr == 1:
                        sqlstr = tsql_bevt_ptr1(db_name, schema, table, product)
                    elif ptr == 2:
                        sqlstr = tsql_bevt_ptr2(db_name, schema, table, product)
                    elif ptr == 3:
                        sqlstr = tsql_bevt_ptr3(db_name, schema, table, product)
                    elif ptr == 4:
                        sqlstr = tsql_bevt_ptr4(db_name, schema, table, product)
                    elif ptr == 5:
                        sqlstr = tsql_bevt_ptr5(db_name, schema, table, product)
                    elif ptr == 6:
                        sqlstr = tsql_bevt_ptr6(db_name, schema, table, product)
                    elif ptr == 7:
                        sqlstr = tsql_bevt_ptr7(db_name, schema, table, product)
                    elif ptr == 8:
                        sqlstr = tsql_bevt_ptr8(db_name, schema, table, product)
                if ba == 'a':
                    if ptr == 1:
                        sqlstr = tsql_aevt_ptr1(db_name, schema, table, product)
                    elif ptr == 2:
                        sqlstr = tsql_aevt_ptr2(db_name, schema, table, product)
                    elif ptr == 3:
                        sqlstr = tsql_aevt_ptr3(db_name, schema, table, product)
                    elif ptr == 4:
                        sqlstr = tsql_aevt_ptr4(db_name, schema, table, product)
                    elif ptr == 5:
                        sqlstr = tsql_aevt_ptr5(db_name, schema, table, product)
                    elif ptr == 6:
                        sqlstr = tsql_aevt_ptr6(db_name, schema, table, product)
                    elif ptr == 7:
                        sqlstr = tsql_aevt_ptr7(db_name, schema, table, product)
                    elif ptr == 8:
                        sqlstr = tsql_aevt_ptr8(db_name, schema, table, product)
                
                db_cursor  = db_util.mssql_exec(server_instance, login_user, login_pass, db_name, sqlstr)
                fetchedall = db_cursor.fetchall()
                evt_time_list = db_util.get_column(db_cursor, 'DateTime', fetchedall)
                dur_list = db_util.get_column(db_cursor, 'Duration', fetchedall)
                mp_list = db_util.get_column(db_cursor, 'MicroPrice', fetchedall)
                #data_frame = db_util.make_dataFrame(db_cursor)
                # summary
                # http://sinhrks.hatenablog.com/entry/2014/10/13/005327
                #daily_evt_freq = data_frame.groupby('Date')['ActType'].count()
                #print(daily_evt_freq)
                #print(data_frame)
                #evt_time_list = data_frame['DateTime'].astype('str').tolist()
                #dur_list = data_frame['Duration'].astype(int).tolist()
                # post
                if len(evt_time_list) == 0:
                    print("skipped {0} {1} {2} {3}".format(db_name,product,ba,ptr))
                    continue
                sqlstr = tsql_microprice(db_name, schema, table, product, evt_time_list[0])
                db_cursor  = db_util.mssql_exec(server_instance, login_user, login_pass, db_name, sqlstr)
                #df = db_util.make_dataFrame(db_cursor)
                fetchedall = db_cursor.fetchall()
                post_evt_list = db_util.get_column(db_cursor, 'DateTime', fetchedall)
                post_mp_list = db_util.get_column(db_cursor, 'MicroPrice', fetchedall)

                start = time.time()
                cep = CppStats.postEvtStat()
                cep.set_post_evts(post_evt_list, post_mp_list)
                ratio = [cep.get_post_evt_stat(evt_time, benchmark, post_mp_list, interval_in_sec) for evt_time, benchmark in zip(evt_time_list, mp_list)]
                end = time.time()
                print("exec post evt takes " , end - start," sec")
                #dur = [tpl[0] for tpl in a]
                #ratio = [tpl[1] for tpl in a]
                d = pd.DataFrame()
                d['DateTime'] = evt_time_list[:len(ratio)]
                d['product'] = [product] * len(ratio)
                d['ptr'] = [ptr] * len(ratio)
                d['ba'] = [ba] * len(ratio)
                d['duration'] = dur_list
                d['uratio'] = ratio
                c = c.append(d)
        
        c.to_csv('./{0}_evt_ptr_{1}.csv'.format(product, db_name), index = False)
  
# ref:　http://stackoverflow.com/questions/11968689/python-multithreading-wait-till-all-threads-finished
import threading 
m_thread1 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(1,))
m_thread2 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(2,))
m_thread3 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(3,))
m_thread4 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(4,))
m_thread5 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(5,))
m_thread6 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(6,))
m_thread7 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(7,))
m_thread8 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(8,))

m_thread1.start()
m_thread2.start()
m_thread3.start()
m_thread4.start()
m_thread5.start()
m_thread6.start()
m_thread7.start()
m_thread8.start()

m_thread1.join()
m_thread2.join()
m_thread3.join()
m_thread4.join()
m_thread5.join()
m_thread6.join()
m_thread7.join()
m_thread8.join()

print()
#calc_all_ptr()

# On Windosw the subprocesses will import (i.e. execute) the main module at starrt. You need to
# protect the main code like this to avoid creating subprocesses recursivel
#if __name__ == '__main__':         
#    func = calc_all_ptr
#    gn_util.multi_proc(func)

#data=[('smith, bob',2),('carol',3),('ted',4),('alice',5)]

#with open('./bevt_ptr1.csv','w') as out:
#    csv_out=csv.writer(out)
#    csv_out.writerow(['duration','up raitio'])
#    for row in a:
#        csv_out.writerow(row)

#import csv
#RESULT = ['apple','cherry','orange','pineapple','strawberry']
#resultFile = open("output.csv",'wb')
#wr = csv.writer(resultFile)
#wr.writerow(RESULT)

# gui plot
#gui_util.hist_plot(r)
#gui_util.scatter_plot(d, r)
#gui_util.slider_control()