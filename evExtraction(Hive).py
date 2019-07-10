class evExtraction(object):
    """description of class"""

import sys
#sys.path.append('../x64/Debug/')
sys.path.append('../x64/Release/')
sys.path.append('../PyUtility/')

import time
import datetime

import subprocess
from subprocess import check_output

import DbUtility as db_util
import IoUtility as io_util
import DataTypeUtility as dpu
import DatetimeUtility as dt_util
import FormatUtility as fmt_util
import GeneralUtility as gn_util
#import GuiUtility as gui_util
from numba import jit

# BoostPython
import CppStats

mecab64 = CppStats.MeCab64()
mecab64.Start("ジョニーは戦場へ行った。".encode('shift-jis'))

cabocha64 = CppStats.CaboCha64()
cabocha64.Start("米シティグループが世界の為替取引の市場シェアで３年連続トップだったことが、ユーロマネー・インスティテューショナル・インベスター誌の調査で明らかになった。".encode('shift-jis'))

def tsql_bevt_ptr1(_db_name , _table, _product):
    # -- pattern.1
    # -- BP.↑[--this duration--]TP.↓
    #  -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
        USE {0}; \n
       
        SELECT
        *
        FROM
        (
          SELECT          
          (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
          *
          FROM
          (
	        SELECT
	        *
	        FROM
	        (
	          SELECT
	          *,
	          LAG(act,1,null) OVER(ORDER BY dt,id) AS PrevAct,
	          LAG(dt,1,null) OVER(ORDER BY dt,id) AS PrevDT,
	          LAG(chg,1,null) OVER(ORDER BY dt,id) AS PrevChg
	          FROM
	          (
		        SELECT
		        chg,
		        act,
		        val,
		        dt,
		        id,
		        mp
		        FROM {1}
		        WHERE ((act = 'TQ.d' ) OR (act = 'BP' and chg > 0)) AND smb = '{2}'
	          ) AS NEXT1
	        ) AS NEXT2
          ) AS NEST3
        ) AS NEST4
        WHERE act <> PrevAct and act = 'TQ.d' and Duration >= 0
        ORDER BY dt;
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_bevt_ptr2(_db_name , _table, _product):
    # -- pattern.2
    # -- TP.↓, BP.↓[--this duration--]BP.↑
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive  

    sqlstr = """
        USE {0};

        SELECT
        *
        FROM
        (
          SELECT          
          (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
          *
          FROM
          (
	        SELECT
	        *
	        FROM
	        (
	          SELECT
	          *,
	          LAG(act,1,null) OVER(ORDER BY dt,id) AS PrevAct,
	          LAG(dt,1,null) OVER(ORDER BY dt,id) AS PrevDT,
	          LAG(chg,1,null) OVER(ORDER BY dt,id) AS PrevChg,
	          LAG(act,2,null) OVER(ORDER BY dt,id) AS PPAct,
	          LAG(dt,2,null) OVER(ORDER BY dt,id) AS PPDatetime,
	          LAG(chg,2,null) OVER(ORDER BY dt,id) AS PPChg
	          FROM
	          (
		        SELECT
		        chg
		        ,act
		        ,val
		        ,dt
		        ,id
		        ,mp
		        FROM {1}
		        WHERE ((act = 'TP.d' ) OR (act = 'BP')) AND smb = '{2}'
	          ) AS NEXT1
	        ) AS NEST2
          ) AS NEST3
        ) AS NEST4
        WHERE PPAct = 'TP.d' AND (PrevAct = 'BP' and PrevChg < 0) AND (act = 'BP' and chg > 0) AND Duration  >= 0
        ORDER BY dt;
    """.format(_db_name, _table, _product)
    return sqlstr

def tsql_bevt_ptr3(_db_name , _table, _product):

    # -- pattern.3
    # -- BP.↑[--this duration--]BP.↓    
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive  

    sqlstr = """
        USE {0};

        SELECT
        *
        FROM
        (
          SELECT          
          (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
          *
          FROM
          (
	        SELECT
	        *
	        FROM
	        (
	          SELECT
	          *,
	          LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
	          LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
	          LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg
	          FROM
	          (
		        SELECT
		        chg,
		        act,
		        val,
		        dt,                  
		        id,
		        mp
		        FROM {1}
		        WHERE ((act = 'AP') and (chg < 0)) AND smb = '{2}'
	          ) AS NEXT1
	        ) AS NEST2
          ) AS NEST3
        ) AS NEST4
        WHERE Duration >= 0
        ORDER BY dt;
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_bevt_ptr4(_db_name , _table, _product):

    # -- pattern.4
    # -- TP↓BP↓[--this duration,PriceChg--]TP↑
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
	    (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *,
	    val - PrevValue AS ValChg
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg,
			    LAG(val,1,null) OVER(ORDER BY dt, id) AS PrevValue,
			    LAG(act,2,null) OVER(ORDER BY dt, id) AS PPAct,
			    LAG(dt,2,null) OVER(ORDER BY dt, id) AS PPDatetime,
			    LAG(chg,2,null) OVER(ORDER BY dt, id) AS PPChg
			    FROM
			    (
				    SELECT
					    chg,
		                act,
		                val,
		                dt,
		                id,
		                mp
				    FROM {1}
				    WHERE ((act LIKE 'TP.%' ) OR (act = 'BP')) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE (PPAct = 'TP.d') AND ((PrevAct = 'BP') and (PrevChg < 0)) AND (act = 'TP.u') AND (Duration  >= 0)
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_bevt_ptr5(_db_name , _table, _product):

    # -- pattern.5
    # -- TP↓TP↓BP↓[--this duration--]AP↓
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT 
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,	
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg	
			    FROM
			    (
				    SELECT
					    chg,
		                act,
		                val,
		                dt,
		                id,
		                mp
				    FROM {1}
				    WHERE ((act = 'AP' and chg < 0 ) OR (act = 'BP' and chg < 0)) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE act <> PrevAct and act = 'AP' and Duration  >= 0
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_bevt_ptr6(_db_name , _table, _product):

    # -- pattern.6
    # -- TP↑TP↑<-TotalQty,
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};
    
    SELECT
    *
    FROM
    (
	    SELECT	    
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg,
			    LAG(val,1,null) OVER(ORDER BY dt, id) AS PrevValue
			    FROM
			    (
				    SELECT
					    chg,
					    act,
					    val,
					    dt,
					    id,
                        mp
				    FROM {1}
				    WHERE (act = 'TQ.u' ) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE (val + PrevValue > 20) and Duration < 20
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_bevt_ptr7(_db_name , _table, _product):

    # -- pattern.7
    # -- BP↓AP↓[--this duration,PriceChg--]AP↑
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
	    (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *,
	    val - PrevValue AS ValChg
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg,
			    LAG(val,1,null) OVER(ORDER BY dt, id) AS PrevValue,
			    LAG(act,2,null) OVER(ORDER BY dt, id) AS PPAct,
			    LAG(dt,2,null) OVER(ORDER BY dt, id) AS PPDatetime,
			    LAG(chg,2,null) OVER(ORDER BY dt, id) AS PPChg
			    FROM
			    (
				    SELECT
					    chg,
					    act,
					    val,
					    dt,
					    id,
                        mp
				    FROM {1}
				    WHERE ((act = 'AP') OR (act = 'BP' and chg < 0)) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'BP' AND (PrevAct = 'AP' and PrevChg < 0) AND (act = 'AP' and chg > 0) AND Duration  >= 0
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_bevt_ptr8(_db_name , _table, _product):

    # -- pattern.8
    # -- TP↓[--this duration--]AP↑
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};
    
    SELECT
    *
    FROM
    (
	    SELECT	    
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(val,1,null) OVER(ORDER BY dt, id) AS PrevValue,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg
			    FROM
			    (
				    SELECT
					    chg,
					    act,
					    val,
					    dt,
					    id,
                        mp
				    FROM {1}
				    WHERE ((act = 'TQ.d' ) OR (act = 'AP' and chg > 0)) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PrevAct = 'TQ.d' and act = 'AP' and Duration > 1
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_aevt_ptr1(_db_name , _table, _product):

    # -- pattern.1
    # -- AP↓[--this duration--]TP↑
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg
			    FROM
			    (
				    SELECT
					    chg,
					    act,
					    val,
					    dt,
					    id,
                        mp
				    FROM {1}
				    WHERE ((act = 'TQ.u' ) OR (act = 'AP' and chg < 0)) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE act <> PrevAct and act = 'TQ.u' and Duration >= 0
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_aevt_ptr2(_db_name , _table, _product):

    # -- pattern.2
    # -- TP↑AP↑[--this duration--]AP↓
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg,
			    LAG(act,2,null) OVER(ORDER BY dt, id) AS PPAct,
			    LAG(dt,2,null) OVER(ORDER BY dt, id) AS PPDatetime,
			    LAG(chg,2,null) OVER(ORDER BY dt, id) AS PPChg
			    FROM
			    (
				    SELECT
                    chg
		            ,act
		            ,val
		            ,dt
		            ,id
		            ,mp
				    FROM {1}
				    WHERE ((act = 'TP.u' ) OR (act = 'AP')) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'TP.u' AND (PrevAct = 'AP' and PrevChg < 0) AND (act = 'AP' and chg > 0) AND Duration  >= 0
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_aevt_ptr3(_db_name , _table, _product):

    # -- pattern.3
    # -- AP↓[--this duration--]AP↓
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg
			    FROM
			    (
				    SELECT
					    chg,
					    act,
					    val,
					    dt,
					    id,
                        mp
				    FROM {1}
				    WHERE ((act = 'AP') and (chg < 0)) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE Duration >= 0
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_aevt_ptr4(_db_name , _table, _product):

    # -- pattern.4
    # -- TP↑AP↑[--this duration,PriceChg--]TP↓
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
	    (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *,
	    val - PrevValue AS ValChg
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg,
			    LAG(val,1,null) OVER(ORDER BY dt, id) AS PrevValue,
			    LAG(act,2,null) OVER(ORDER BY dt, id) AS PPAct,
			    LAG(dt,2,null) OVER(ORDER BY dt, id) AS PPDatetime,
			    LAG(chg,2,null) OVER(ORDER BY dt, id) AS PPChg
			    FROM
			    (
				    SELECT
					    chg,
		                act,
		                val,
		                dt,
		                id,
		                mp
				    FROM {1}
				    WHERE ((act LIKE 'TP.%' ) OR (act = 'AP')) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'TP.u' AND (PrevAct = 'AP' and PrevChg < 0) AND act = 'TP.d' AND Duration  >= 0
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_aevt_ptr5(_db_name , _table, _product):

    # -- pattern.5
    # -- TP↑TP↑AP↑[--this duration--]BP↑
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT 
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,	
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg	
			    FROM
			    (
				    SELECT
					    chg,
		                act,
		                val,
		                dt,
		                id,
		                mp
				    FROM {1}
				    WHERE ((act = 'BP' and chg > 0 ) OR (act = 'AP' and chg > 0)) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE act <> PrevAct and act = 'BP' and Duration  >= 0
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_aevt_ptr6(_db_name , _table, _product):

    # -- pattern.6
    # -- TP↓TP↓<-TotalQty,
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg,
			    LAG(val,1,null) OVER(ORDER BY dt, id) AS PrevValue
			    FROM
			    (
				    SELECT
					    chg,
					    act,
					    val,
					    dt,
					    id,
                        mp
				    FROM {1}
				    WHERE (act = 'TQ.d' ) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE (val + PrevValue > 20) and Duration < 20
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_aevt_ptr7(_db_name , _table, _product):

    # -- pattern.7
    # -- AP↑BP↑[--this duration,PriceChg--]BP↓
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
	    (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *,
	    val - PrevValue AS ValChg
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg,
			    LAG(val,1,null) OVER(ORDER BY dt, id) AS PrevValue,
			    LAG(act,2,null) OVER(ORDER BY dt, id) AS PPAct,
			    LAG(dt,2,null) OVER(ORDER BY dt, id) AS PPDatetime,
			    LAG(chg,2,null) OVER(ORDER BY dt, id) AS PPChg
			    FROM
			    (
				    SELECT
					    chg,
					    act,
					    val,
					    dt,
					    id,
                        mp
				    FROM {1}
				    WHERE ((act = 'BP') OR (act = 'AP' and chg > 0)) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'AP' AND (PrevAct = 'BP' and PrevChg > 0) AND (act = 'BP' and chg < 0) AND Duration  >= 0
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_aevt_ptr8(_db_name , _table, _product):
    
    # -- pattern.8
    # -- TP↑[--this duration--]BP↓
    # -- time diff in sec. http://stackoverflow.com/questions/33593080/how-to-get-date-difference-in-minutes-using-hive

    sqlstr = """
    USE {0};

    SELECT
    *
    FROM
    (
	    SELECT	    
        (unix_timestamp(dt) - unix_timestamp(PrevDT))/3600 AS Duration,
        *
	    FROM
	    (
		    SELECT
		    *
		    FROM
		    (
			    SELECT
			    *,
			    LAG(act,1,null) OVER(ORDER BY dt, id) AS PrevAct,
			    LAG(val,1,null) OVER(ORDER BY dt, id) AS PrevValue,
			    LAG(dt,1,null) OVER(ORDER BY dt, id) AS PrevDT,
			    LAG(chg,1,null) OVER(ORDER BY dt, id) AS PrevChg
			    FROM
			    (
				    SELECT
					    chg,
					    act,
					    val,
					    dt,
					    id,
                        mp
				    FROM {1}
				    WHERE ((act = 'TQ.u' ) OR (act = 'BP' and chg < 0)) AND smb = '{2}'
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PrevAct = 'TQ.u' and act = 'BP' and Duration > 1
    ORDER BY dt
    """.format(_db_name , _table, _product)
    return sqlstr

def tsql_trades(_db_name, _table, _product, _evt_time):
    sqlstr = """
    USE {0};
    SELECT dt, act, val, mp
	FROM {1}
	WHERE act LIKE 'TP%' and smb = '{2}' and dt >= '{3}'
    ORDER BY dt
    """.format(_db_name, _table, _product, _evt_time)
    return sqlstr

def tsql_microprice(_db_name, _table, _product, _evt_time):
    sqlstr = """
    USE {0};
    SELECT dt, act, val, mp
	FROM {1}
	WHERE act LIKE '%P%' and smb = '{2}' and dt > '{3}'
    ORDER BY dt
    """.format(_db_name, _table, _product, _evt_time)
    return sqlstr

def tsql_leadtime(_db_name, _table, _product, _evt_time, _num_of_obs=100):
    sqlstr = """
    USE {0};
    SELECT TOP 1 
	LEAD(dt,{0}-1,null) OVER(ORDER BY dt) AS Next100TradeTimestamp 
	FROM {1}
	WHERE act LIKE 'TP%' and Symbol = '{3}' and dt >= '{4}'
    """.format(_num_of_obs, _db_name, _table, _product, _evt_time)
    return sqlstr

def tsql_benchprice(_db_name, _table, _product, _evt_time):    # find the microprice nearest to an evt
    sqlstr = """
    USE {0};
    SELECT TOP 1
    mp,dt
    FROM {1}
    WHERE smb = '{2}' and dt < '{3}'
    ORDER BY dt DESC
    """.format(_db_name, _table, _product, _evt_time)
    return sqlstr

def tsql_post_evt_stat(_db_name, _table, _product, _benchprice, _evt_time, _lead_time, _num_of_obs):
    sqlstr = """
    USE {0};
    SELECT 
	COUNT(CASE WHEN val > {3} THEN 1 END) / ({6} * 1.0) AS UpProb,
	COUNT(CASE WHEN val < {3} THEN 1 END) / ({6} * 1.0) AS DnProb
	FROM {1}
	WHERE smb = '{2}' AND (dt Between '{4}' and '{5}') AND act LIKE 'TP%'
    """.format(_db_name, _table, _product, _benchprice, _evt_time, _lead_time, _num_of_obs)
    return sqlstr

import OsUtility as os_util
server_instance = "P11013" if os_util.get_user() == "operator" else "STEVE-PC"
login_user = "sa"
login_pass = "Bigdata01"
#product_list = ["1321-TSE",] if os_util.get_user() == "operator" else ["THU N6-TCM"]
product_list = [
  #      "1308-TSE",
		#"1311-TSE",
		#"1314-TSE",
		#"1321-TSE", 
		#"1322-TSE",
		#"1323-TSE",
		#"1324-TSE",
		#"1325-TSE",
		#"1326-TSE",
		#"1328-TSE",
		#"1330-TSE",
		#"1333-TSE",
		#"1345-TSE",
		#"1348-TSE",
		#"1352-TSE",
		#"1358-TSE",
		#"1360-TSE",
		#"1361-TSE",
		#"1362-TSE",
		#"1363-TSE",
		#"1366-TSE",
		#"1367-TSE",
		#"1368-TSE",
		#"1369-TSE",
		#"1376-TSE",
		#"1377-TSE",
		#"1379-TSE",
		#"1381-TSE",
		#"1384-TSE",
		#"1385-TSE",
		#"1386-TSE",
		#"1387-TSE",
		#"1388-TSE",
		#"1389-TSE",
		#"1390-TSE",
		#"1391-TSE",
		#"1392-TSE",
		#"1393-TSE",
		#"1394-TSE",
		#"1397-TSE",
		#"1398-TSE",
		#"1399-TSE",
		#"1401-TSE",
		#"1407-TSE",
		#"1414-TSE",
		#"1417-TSE",
		#"1420-TSE",
		#"1429-TSE",
		#"1430-TSE",
		#"1433-TSE",
		#"1434-TSE",
		#"1435-TSE",
		#"1456-TSE",
		#"1457-TSE",
		#"1459-TSE",
		#"1464-TSE",
		#"1465-TSE",
		#"1466-TSE",
		#"1467-TSE",
		#"1468-TSE",
		#"1470-TSE",
		#"1471-TSE",
		#"1472-TSE",
		#"1473-TSE",
		#"1474-TSE",
		#"1475-TSE",
		#"1476-TSE",
		#"1477-TSE",
		#"1478-TSE",
		#"1491-TSE",
		#"1514-TSE",
		#"1518-TSE",
		#"1540-TSE",
		#"1541-TSE",
		#"1543-TSE",
		#"1545-TSE",
		#"1546-TSE",
		#"1548-TSE",
		#"1549-TSE",
		#"1551-TSE",
		#"1552-TSE",
		#"1554-TSE",
		#"1555-TSE",
		#"1559-TSE",
		#"1560-TSE",
		#"1561-TSE",
		#"1569-TSE",
		#"1571-TSE",
		#"1572-TSE",
		#"1574-TSE",
		#"1577-TSE",
		#"1580-TSE",
		#"1582-TSE",
		#"1583-TSE",
		#"1584-TSE",
		#"1585-TSE",
		#"1587-TSE",
		#"1588-TSE",
		#"1589-TSE",
		#"1590-TSE",
		#"1591-TSE",
		#"1592-TSE",
		#"1595-TSE",
		#"1596-TSE",
		#"1597-TSE",
		#"1598-TSE",
		#"1610-TSE",
		#"1612-TSE",
		#"1613-TSE",
		#"1617-TSE",
		#"1618-TSE",
		#"1619-TSE",
		#"1621-TSE",
		#"1624-TSE",
		#"1625-TSE",
		#"1626-TSE",
		#"1627-TSE",
		#"1628-TSE",
		#"1629-TSE",
		#"1630-TSE",
		#"1631-TSE",
		#"1632-TSE",
		#"1633-TSE",
		#"1634-TSE",
		#"1635-TSE",
		#"1636-TSE",
		#"1637-TSE",
		#"1638-TSE",
		#"1639-TSE",
		#"1640-TSE",
		#"1641-TSE",
		#"1642-TSE",
		#"1643-TSE",
		#"1644-TSE",
		#"1645-TSE",
		#"1646-TSE",
		#"1647-TSE",
		#"1649-TSE",
		#"1650-TSE",
		#"1662-TSE",
		#"1663-TSE",
		#"1671-TSE",
		#"1677-TSE",
		#"1678-TSE",
		#"1679-TSE",
		#"1680-TSE",
		#"1689-TSE",
		#"1690-TSE",
		#"1695-TSE",
		#"1696-TSE",
		#"1698-TSE",
		#"1699-TSE",
		#"1711-TSE",
		#"1716-TSE",
		#"1717-TSE",
		#"1719-TSE",
		#"1723-TSE",
		#"1724-TSE",
		#"1726-TSE",
		#"1730-TSE",
		#"1736-TSE",
		#"1737-TSE",
		#"1739-TSE",
		#"1762-TSE",
		#"1766-TSE",
		#"1775-TSE",
		#"1776-TSE",
		#"1782-TSE",
		#"1783-TSE",
		#"1799-TSE",
		#"1801-TSE",
		#"1802-TSE",
		#"1805-TSE",
		#"1808-TSE",
		#"1810-TSE",
		#"1813-TSE",
		#"1820-TSE",
		#"1824-TSE",
		#"1826-TSE",
		#"1828-TSE",
		#"1835-TSE",
		#"1840-TSE",
		#"1844-TSE",
		#"1850-TSE",
		#"1853-TSE",
		#"1860-TSE",
		#"1861-TSE",
		#"1867-TSE",
		#"1868-TSE",
		#"1870-TSE",
		#"1871-TSE",
		#"1873-TSE",
		#"1879-TSE",
		#"1881-TSE",
		#"1884-TSE",
		#"1885-TSE",
		#"1888-TSE",
		#"1893-TSE",
		#"1896-TSE",
		#"1905-TSE",
		#"1906-TSE",
		#"1909-TSE",
		#"1911-TSE",
		#"1914-TSE",
		#"1916-TSE",
		#"1921-TSE",
		#"1929-TSE",
		#"1930-TSE",
		#"1933-TSE",
		#"1934-TSE",
		#"1935-TSE",
		#"1938-TSE",
		#"1939-TSE",
		#"1941-TSE",
		#"1942-TSE",
		#"1944-TSE",
		#"1945-TSE",
		#"1946-TSE",
		#"1949-TSE",
		#"1951-TSE",
		#"1956-TSE",
		#"1960-TSE",
		#"1963-TSE",
		#"1964-TSE",
		#"1965-TSE",
		#"1966-TSE",
		#"1967-TSE",
		#"1969-TSE",
		#"1971-TSE",
		#"1972-TSE",
		#"1975-TSE",
		#"1979-TSE",
		#"1980-TSE",
		#"1981-TSE",
		#"1982-TSE",
		#"1983-TSE",
		#"1994-TSE",
		#"1997-TSE",
		#"2001-TSE",
		#"2002-TSE",
		#"2003-TSE",
		#"2032-TSE",
		#"2033-TSE",
		#"2035-TSE",
		#"2036-TSE",
		#"2037-TSE",
		#"2039-TSE",
		#"2040-TSE",
		#"2041-TSE",
		#"2043-TSE",
		#"2044-TSE",
		#"2045-TSE",
		#"2046-TSE",
		#"2047-TSE",
		#"2049-TSE",
		#"2053-TSE",
		#"2060-TSE",
		#"2107-TSE",
		#"2114-TSE",
		#"2117-TSE",
		#"2120-TSE",
		#"2122-TSE",
		#"2127-TSE",
		#"2131-TSE",
		#"2134-TSE",
		#"2138-TSE",
		#"2139-TSE",
		#"2148-TSE",
		#"2150-TSE",
		#"2152-TSE",
		#"2153-TSE",
		#"2154-TSE",
		#"2156-TSE",
		#"2157-TSE",
		#"2158-TSE",
		#"2162-TSE",
		#"2163-TSE",
		#"2164-TSE",
		#"2169-TSE",
		#"2170-TSE",
		#"2173-TSE",
		#"2176-TSE",
		#"2178-TSE",
		#"2180-TSE",
		#"2181-TSE",
		#"2186-TSE",
		#"2191-TSE",
		#"2193-TSE",
		#"2196-TSE",
		#"2198-TSE",
		#"2204-TSE",
		#"2207-TSE",
		#"2215-TSE",
		#"2216-TSE",
		#"2217-TSE",
		#"2220-TSE",
		#"2222-TSE",
		#"2224-TSE",
		#"2226-TSE",
		#"2228-TSE",
		#"2267-TSE",
		#"2270-TSE",
		#"2281-TSE",
		#"2286-TSE",
		#"2288-TSE",
		#"2292-TSE",
		#"2294-TSE",
		#"2303-TSE",
		#"2307-TSE",
		#"2309-TSE",
		#"2311-TSE",
		#"2315-TSE",
		#"2323-TSE",
		#"2325-TSE",
		#"2327-TSE",
		#"2337-TSE",
		#"2345-TSE",
		#"2349-TSE",
		#"2352-TSE",
		#"2354-TSE",
		#"2359-TSE",
		#"2362-TSE",
		#"2375-TSE",
		#"2376-TSE",
		#"2378-TSE",
		#"2379-TSE",
		#"2385-TSE",
		#"2388-TSE",
		#"2389-TSE",
		#"2393-TSE",
		#"2397-TSE",
		#"2398-TSE",
		#"2402-TSE",
		#"2404-TSE",
		#"2410-TSE",
		#"2411-TSE",
		#"2412-TSE",
		#"2415-TSE",
		#"2418-TSE",
		#"2427-TSE",
		#"2428-TSE",
		#"2429-TSE",
		#"2432-TSE",
		#"2433-TSE",
		#"2436-TSE",
		#"2437-TSE",
		#"2440-TSE",
		#"2445-TSE",
		#"2449-TSE",
		#"2459-TSE",
		#"2461-TSE",
		#"2462-TSE",
		#"2464-TSE",
		#"2471-TSE",
		#"2475-TSE",
		#"2477-TSE",
		#"2483-TSE",
		#"2485-TSE",
		#"2487-TSE",
		#"2488-TSE",
		#"2491-TSE",
		#"2492-TSE",
		#"2493-TSE",
		#"2498-TSE",
		#"2499-TSE",
		#"2501-TSE",
		#"2503-TSE",
		#"2531-TSE",
		#"2538-TSE",
		#"2540-TSE",
		#"2580-TSE",
		#"2586-TSE",
		#"2587-TSE",
		#"2590-TSE",
		#"2593P-TSE",
		#"2593-TSE",
		#"2597-TSE",
		#"2599-TSE",
		#"2607-TSE",
		#"2612-TSE",
		#"2652-TSE",
		#"2654-TSE",
		#"2655-TSE",
		#"2659-TSE",
		#"2666-TSE",
		#"2667-TSE",
		#"2668-TSE",
		#"2670-TSE",
		#"2673-TSE",
		#"2674-TSE",
		#"2675-TSE",
		#"2676-TSE",
		#"2683-TSE",
		#"2685-TSE",
		#"2686-TSE",
		#"2687-TSE",
		#"2692-TSE",
		#"2693-TSE",
		#"2694-TSE",
		#"2695-TSE",
		#"2698-TSE",
		#"2703-TSE",
		#"2706-TSE",
		#"2719-TSE",
		#"2721-TSE",
		#"2726-TSE",
		#"2729-TSE",
		#"2730-TSE",
		#"2733-TSE",
		#"2734-TSE",
		#"2737-TSE",
		#"2742-TSE",
		#"2743-TSE",
		#"2750-TSE",
		#"2751-TSE",
		#"2752-TSE",
		#"2753-TSE",
		#"2760-TSE",
		#"2762-TSE",
		#"2763-TSE",
		#"2764-TSE",
		#"2767-TSE",
		#"2772-TSE",
		#"2773-TSE",
		#"2778-TSE",
		#"2780-TSE",
		#"2782-TSE",
		#"2784-TSE",
		#"2789-TSE",
		#"2790-TSE",
		#"2796-TSE",
		#"2798-TSE",
		#"2801-TSE",
		#"2805-TSE",
		#"2806-TSE",
		#"2812-TSE",
		#"2814-TSE",
		#"2815-TSE",
		#"2816-TSE",
		#"2818-TSE",
		#"2819-TSE",
		#"2830-TSE",
		#"2831-TSE",
		#"2871-TSE",
		#"2872-TSE",
		#"2876-TSE",
		#"2882-TSE",
		#"2883-TSE",
		#"2903-TSE",
		#"2904-TSE",
		#"2910-TSE",
		#"2915-TSE",
		#"2918-TSE",
		#"2922-TSE",
		#"2923-TSE",
		#"2925-TSE",
		#"2926-TSE",
		#"2927-TSE",
		#"2929-TSE",
		#"2930-TSE",
		#"2931-TSE",
		#"3002-TSE",
		#"3004-TSE",
		#"3010-TSE",
		#"3021-TSE",
		#"3022-TSE",
		#"3023-TSE",
		#"3030-TSE",
		#"3031-TSE",
		#"3034-TSE",
		#"3035-TSE",
		#"3040-TSE",
		#"3041-TSE",
		#"3045-TSE",
		#"3054-TSE",
		#"3059-TSE",
		#"3063-TSE",
		#"3067-TSE",
		#"3068-TSE",
		#"3070-TSE",
		#"3075-TSE",
		#"3076-TSE",
		#"3077-TSE",
		#"3079-TSE",
		#"3080-TSE",
		#"3082-TSE",
		#"3085-TSE",
		#"3086-TSE",
		#"3088-TSE",
		#"3092-TSE",
		#"3096-TSE",
		#"3098-TSE",
		#"3099-TSE",
		#"3103-TSE",
		#"3104-TSE",
		#"3105-TSE",
		#"3107-TSE",
		#"3109-TSE",
		#"3110-TSE",
		#"3111-TSE",
		#"3113-TSE",
		#"3116-TSE",
		#"3121-TSE",
		#"3131-TSE",
		#"3132-TSE",
		#"3133-TSE",
		#"3134-TSE",
		#"3137-TSE",
		#"3140-TSE",
		#"3150-TSE",
		#"3153-TSE",
		#"3154-TSE",
		#"3156-TSE",
		#"3157-TSE",
		#"3159-TSE",
		#"3160-TSE",
		#"3161-TSE",
		#"3165-TSE",
		#"3166-TSE",
		#"3167-TSE",
		#"3168-TSE",
		#"3169-TSE",
		#"3171-TSE",
		#"3172-TSE",
		#"3173-TSE",
		#"3176-TSE",
		#"3177-TSE",
		#"3180-TSE",
		#"3182-TSE",
		#"3185-TSE",
		#"3186-TSE",
		#"3189-TSE",
		#"3190-TSE",
		#"3191-TSE",
		#"3192-TSE",
		#"3195-TSE",
		#"3198-TSE",
		#"3199-TSE",
		#"3204-TSE",
		#"3205-TSE",
		#"3221-TSE",
		#"3222-TSE",
		#"3223-TSE",
		#"3224-TSE",
		#"3226-TSE",
		#"3227-TSE",
		#"3230-TSE",
		#"3231-TSE",
		#"3232-TSE",
		#"3236-TSE",
		#"3237-TSE",
		#"3238-TSE",
		#"3241-TSE",
		#"3242-TSE",
		#"3244-TSE",
		#"3249-TSE",
		#"3252-TSE",
		#"3264-TSE",
		#"3266-TSE",
		#"3271-TSE",
		#"3276-TSE",
		#"3277-TSE",
		#"3278-TSE",
		#"3280-TSE",
		#"3281-TSE",
		#"3282-TSE",
		#"3283-TSE",
		#"3284-TSE",
		#"3286-TSE",
		#"3287-TSE",
		#"3289-TSE",
		#"3291-TSE",
		#"3293-TSE",
		#"3294-TSE",
		#"3295-TSE",
		#"3297-TSE",
		#"3298-TSE",
		#"3299-TSE",
		#"3308-TSE",
		#"3313-TSE",
		#"3316-TSE",
		#"3319-TSE",
		#"3320-TSE",
		#"3321-TSE",
		#"3322-TSE",
		#"3328-TSE",
		#"3329-TSE",
		#"3341-TSE",
		#"3344-TSE",
		#"3347-TSE",
		#"3349-TSE",
		#"3350-TSE",
		#"3352-TSE",
		#"3359-TSE",
		#"3360-TSE",
		#"3366-TSE",
		#"3374-TSE",
		#"3376-TSE",
		#"3377-TSE",
		#"3382-TSE",
		#"3385-TSE",
		#"3386-TSE",
		#"3390-TSE",
		#"3393-TSE",
		#"3395-TSE",
		#"3396-TSE",
		#"3397-TSE",
		#"3399-TSE",
		#"3401-TSE",
		#"3405-TSE",
		#"3407-TSE",
		#"3409-TSE",
		#"3418-TSE",
		#"3420-TSE",
		#"3421-TSE",
		#"3423-TSE",
		#"3434-TSE",
		#"3436-TSE",
		#"3439-TSE",
		#"3441-TSE",
		#"3445-TSE",
		#"3451-TSE",
		#"3452-TSE",
		#"3455-TSE",
		#"3457-TSE",
		#"3458-TSE",
		#"3460-TSE",
		#"3461-TSE",
		#"3462-TSE",
		#"3463-TSE",
		#"3464-TSE",
		#"3465-TSE",
		#"3512-TSE",
		#"3513-TSE",
		#"3524-TSE",
		#"3526-TSE",
		#"3528-TSE",
		#"3551-TSE",
		#"3553-TSE",
		#"3569-TSE",
		#"3571-TSE",
		#"3578-TSE",
		#"3604-TSE",
		#"3606-TSE",
		#"3607-TSE",
		#"3608-TSE",
		#"3622-TSE",
		#"3623-TSE",
		#"3625-TSE",
		#"3627-TSE",
		#"3628-TSE",
		#"3630-TSE",
		#"3632-TSE",
		#"3633-TSE",
		#"3634-TSE",
		#"3635-TSE",
		#"3636-TSE",
		#"3639-TSE",
		#"3640-TSE",
		#"3641-TSE",
		#"3646-TSE",
		#"3647-TSE",
		#"3648-TSE",
		#"3649-TSE",
		#"3652-TSE",
		#"3653-TSE",
		#"3654-TSE",
		#"3656-TSE",
		#"3658-TSE",
		#"3659-TSE",
		#"3662-TSE",
		#"3663-TSE",
		#"3664-TSE",
		#"3665-TSE",
		#"3666-TSE",
		#"3669-TSE",
		#"3671-TSE",
		#"3672-TSE",
		#"3677-TSE",
		#"3678-TSE",
		#"3679-TSE",
		#"3680-TSE",
		#"3681-TSE",
		#"3682-TSE",
		#"3683-TSE",
		#"3687-TSE",
		#"3688-TSE",
		#"3689-TSE",
		#"3690-TSE",
		#"3694-TSE",
		#"3695-TSE",
		#"3697-TSE",
		#"3698-TSE",
		#"3710-TSE",
		#"3711-TSE",
		#"3712-TSE",
		#"3723-TSE",
		#"3724-TSE",
		#"3726-TSE",
		#"3727-TSE",
		#"3733-TSE",
		#"3742-TSE",
		#"3744-TSE",
		#"3747-TSE",
		#"3750-TSE",
		#"3758-TSE",
		#"3760-TSE",
		#"3762-TSE",
		#"3763-TSE",
		#"3765-TSE",
		#"3766-TSE",
		#"3768-TSE",
		#"3769-TSE",
		#"3771-TSE",
		#"3772-TSE",
		#"3776-TSE",
		#"3777-TSE",
		#"3778-TSE",
		#"3779-TSE",
		#"3782-TSE",
		#"3784-TSE",
		#"3787-TSE",
		#"3794-TSE",
		#"3796-TSE",
		#"3798-TSE",
		#"3803-TSE",
		#"3804-TSE",
		#"3807-TSE",
		#"3810-TSE",
		#"3814-TSE",
		#"3815-TSE",
		#"3816-TSE",
		#"3817-TSE",
		#"3822-TSE",
		#"3823-TSE",
		#"3825-TSE",
		#"3826-TSE",
		#"3832-TSE",
		#"3835-TSE",
		#"3836-TSE",
		#"3840-TSE",
		#"3842-TSE",
		#"3843-TSE",
		#"3850-TSE",
		#"3851-TSE",
		#"3852-TSE",
		#"3853-TSE",
		#"3856-TSE",
		#"3858-TSE",
		#"3861-TSE",
		#"3863-TSE",
		#"3864-TSE",
		#"3865-TSE",
		#"3877-TSE",
		#"3878-TSE",
		#"3880-TSE",
		#"3891-TSE",
		#"3895-TSE",
		#"3896-TSE",
		#"3900-TSE",
		#"3901-TSE",
		#"3902-TSE",
		#"3904-TSE",
		#"3905-TSE",
		#"3906-TSE",
		#"3907-TSE",
		#"3908-TSE",
		#"3910-TSE",
		#"3912-TSE",
		#"3914-TSE",
		#"3916-TSE",
		#"3917-TSE",
		#"3918-TSE",
		#"3920-TSE",
		#"3921-TSE",
		#"3923-TSE",
		#"3924-TSE",
		#"3925-TSE",
		#"3926-TSE",
		#"3927-TSE",
		#"3928-TSE",
		#"3929-TSE",
		#"3946-TSE",
		#"3950-TSE",
		#"3955-TSE",
		#"4008-TSE",
		#"4021-TSE",
		#"4023-TSE",
		#"4026-TSE",
		#"4028-TSE",
		#"4044-TSE",
		#"4045-TSE",
		#"4078-TSE",
		#"4088-TSE",
		#"4092-TSE",
		#"4093-TSE",
		#"4097-TSE",
		#"4099-TSE",
		#"4100-TSE",
		#"4115-TSE",
		#"4116-TSE",
		#"4120-TSE",
		#"4151-TSE",
		#"4183-TSE",
		#"4202-TSE",
		#"4203-TSE",
		#"4204-TSE",
		#"4205-TSE",
		#"4206-TSE",
		#"4208-TSE",
		#"4212-TSE",
		#"4215-TSE",
		#"4216-TSE",
		#"4218-TSE",
		#"4221-TSE",
		#"4222-TSE",
		#"4224-TSE",
		#"4229-TSE",
		#"4231-TSE",
		#"4234-TSE",
		#"4235-TSE",
		#"4237-TSE",
		#"4238-TSE",
		#"4239-TSE",
		#"4240-TSE",
		#"4241-TSE",
		#"4243-TSE",
		#"4245-TSE",
		#"4246-TSE",
		#"4248-TSE",
		#"4272-TSE",
		#"4275-TSE",
		#"4284-TSE",
		#"4286-TSE",
		#"4287-TSE",
		#"4290-TSE",
		#"4291-TSE",
		#"4293-TSE",
		#"4299-TSE",
		#"4308-TSE",
		#"4310-TSE",
		#"4312-TSE",
		#"4316-TSE",
		#"4317-TSE",
		#"4319-TSE",
		#"4320-TSE",
		#"4321-TSE",
		#"4324-TSE",
		#"4331-TSE",
		#"4335-TSE",
		#"4342-TSE",
		#"4344-TSE",
		#"4346-TSE",
		#"4351-TSE",
		#"4355-TSE",
		#"4364-TSE",
		#"4366-TSE",
		#"4367-TSE",
		#"4368-TSE",
		#"4403-TSE",
		#"4404-TSE",
		#"4410-TSE",
		#"4463-TSE",
		#"4464-TSE",
		#"4465-TSE",
		#"4471-TSE",
		#"4506-TSE",
		#"4508-TSE",
		#"4512-TSE",
		#"4516-TSE",
		#"4517-TSE",
		#"4521-TSE",
		#"4523-TSE",
		#"4524-TSE",
		#"4526-TSE",
		#"4527-TSE",
		#"4530-TSE",
		#"4534-TSE",
		#"4539-TSE",
		#"4541-TSE",
		#"4543-TSE",
		#"4544-TSE",
		#"4547-TSE",
		#"4548-TSE",
		#"4550-TSE",
		#"4551-TSE",
		#"4552-TSE",
		#"4554-TSE",
		#"4555-TSE",
		#"4556-TSE",
		#"4563-TSE",
		#"4564-TSE",
		#"4565-TSE",
		#"4569-TSE",
		#"4570-TSE",
		#"4571-TSE",
		#"4575-TSE",
		#"4576-TSE",
		#"4577-TSE",
		#"4578-TSE",
		#"4581-TSE",
		#"4583-TSE",
		#"4584-TSE",
		#"4586-TSE",
		#"4587-TSE",
		#"4588-TSE",
		#"4592-TSE",
		#"4594-TSE",
		#"4595-TSE",
		#"4611-TSE",
		#"4612-TSE",
		#"4613-TSE",
		#"4615-TSE",
		#"4619-TSE",
		#"4620-TSE",
		#"4627-TSE",
		#"4631-TSE",
		#"4634-TSE",
		#"4642-TSE",
		#"4644-TSE",
		#"4645-TSE",
		#"4650-TSE",
		#"4651-TSE",
		#"4653-TSE",
		#"4657-TSE",
		#"4659-TSE",
		#"4661-TSE",
		#"4664-TSE",
		#"4665-TSE",
		#"4666-TSE",
		#"4667-TSE",
		#"4671-TSE",
		#"4678-TSE",
		#"4679-TSE",
		#"4680-TSE",
		#"4681-TSE",
		#"4684-TSE",
		#"4687-TSE",
		#"4695-TSE",
		#"4708-TSE",
		#"4709-TSE",
		#"4712-TSE",
		#"4718-TSE",
		#"4719-TSE",
		#"4722-TSE",
		#"4728-TSE",
		#"4732-TSE",
		#"4733-TSE",
		#"4734-TSE",
		#"4735-TSE",
		#"4736-TSE",
		#"4739-TSE",
		#"4743-TSE",
		#"4750-TSE",
		#"4762-TSE",
		#"4763-TSE",
		#"4764-TSE",
		#"4765-TSE",
		#"4766-TSE",
		#"4768-TSE",
		#"4770-TSE",
		#"4772-TSE",
		#"4775-TSE",
		#"4776-TSE",
		#"4784-TSE",
		#"4793-TSE",
		#"4801-TSE",
		#"4809-TSE",
		#"4816-TSE",
		#"4819-TSE",
		#"4820-TSE",
		#"4824-TSE",
		#"4828-TSE",
		#"4829-TSE",
		#"4832-TSE",
		#"4833-TSE",
		#"4837-TSE",
		#"4838-TSE",
		#"4839-TSE",
		#"4840-TSE",
		#"4845-TSE",
		#"4849-TSE",
		#"4912-TSE",
		#"4914-TSE",
		#"4917-TSE",
		#"4918-TSE",
		#"4919-TSE",
		#"4920-TSE",
		#"4921-TSE",
		#"4923-TSE",
		#"4924-TSE",
		#"4926-TSE",
		#"4927-TSE",
		#"4928-TSE",
		#"4929-TSE",
		#"4951-TSE",
		#"4952-TSE",
		#"4955-TSE",
		#"4956-TSE",
		#"4958-TSE",
		#"4963-TSE",
		#"4967-TSE",
		#"4968-TSE",
		#"4970-TSE",
		#"4972-TSE",
		#"4973-TSE",
		#"4974-TSE",
		#"4975-TSE",
		#"4978-TSE",
		#"4979-TSE",
		#"4980-TSE",
		#"4987-TSE",
		#"4989-TSE",
		#"4992-TSE",
		#"4994-TSE",
		#"4996-TSE",
		#"4998-TSE",
		#"5002-TSE",
		#"5009-TSE",
		#"5011-TSE",
		#"5012-TSE",
		#"5013-TSE",
		#"5017-TSE",
		#"5019-TSE",
		#"5021-TSE",
		#"5101-TSE",
		#"5103-TSE",
		#"5104-TSE",
		#"5105-TSE",
		#"5110-TSE",
		#"5161-TSE",
		#"5162-TSE",
		#"5184-TSE",
		#"5185-TSE",
		#"5186-TSE",
		#"5192-TSE",
		#"5194-TSE",
		#"5201-TSE",
		#"5202-TSE",
		#"5204-TSE",
		#"5208-TSE",
		#"5210-TSE",
		#"5216-TSE",
		#"5218-TSE",
		#"5232-TSE",
		#"5233-TSE",
		#"5237-TSE",
		#"5261-TSE",
		#"5268-TSE",
		#"5271-TSE",
		#"5273-TSE",
		#"5280-TSE",
		#"5282-TSE",
		#"5284-TSE",
		#"5287-TSE",
		#"5288-TSE",
		#"5289-TSE",
		#"5302-TSE",
		#"5304-TSE",
		#"5331-TSE",
		#"5332-TSE",
		#"5334-TSE",
		#"5337-TSE",
		#"5341-TSE",
		#"5344-TSE",
		#"5351-TSE",
		#"5352-TSE",
		#"5358-TSE",
		#"5363-TSE",
		#"5367-TSE",
		#"5381-TSE",
		#"5384-TSE",
		#"5388-TSE",
		#"5391-TSE",
		#"5393-TSE",
		#"5406-TSE",
		#"5408-TSE",
		#"5410-TSE",
		#"5411-TSE",
		#"5440-TSE",
		#"5444-TSE",
		#"5445-TSE",
		#"5449-TSE",
		#"5451-TSE",
		#"5453-TSE",
		#"5456-TSE",
		#"5458-TSE",
		#"5464-TSE",
		#"5476-TSE",
		#"5486-TSE",
		#"5491-TSE",
		#"5541-TSE",
		#"5542-TSE",
		#"5609-TSE",
		#"5612-TSE",
		#"5631-TSE",
		#"5659-TSE",
		#"5660-TSE",
		#"5698-TSE",
		#"5702-TSE",
		#"5703-TSE",
		#"5706-TSE",
		#"5711-TSE",
		#"5713-TSE",
		#"5714-TSE",
		#"5715-TSE",
		#"5726-TSE",
		#"5727-TSE",
		#"5742-TSE",
		#"5753-TSE",
		#"5781-TSE",
		#"5801-TSE",
		#"5802-TSE",
		#"5803-TSE",
		#"5805-TSE",
		#"5807-TSE",
		#"5809-TSE",
		#"5815-TSE",
		#"5816-TSE",
		#"5817-TSE",
		#"5821-TSE",
		#"5851-TSE",
		#"5856-TSE",
		#"5901-TSE",
		#"5902-TSE",
		#"5903-TSE",
		#"5909-TSE",
		#"5912-TSE",
		#"5915-TSE",
		#"5918-TSE",
		#"5921-TSE",
		#"5922-TSE",
		#"5923-TSE",
		#"5929-TSE",
		#"5930-TSE",
		#"5932-TSE",
		#"5936-TSE",
		#"5938-TSE",
		#"5941-TSE",
		#"5942-TSE",
		#"5946-TSE",
		#"5947-TSE",
		#"5949-TSE",
		#"5950-TSE",
		#"5951-TSE",
		#"5952-TSE",
		#"5956-TSE",
		#"5957-TSE",
		#"5958-TSE",
		#"5962-TSE",
		#"5970-TSE",
		#"5973-TSE",
		#"5974-TSE",
		#"5976-TSE",
		#"5982-TSE",
		#"5984-TSE",
		#"5989-TSE",
		#"5991-TSE",
		#"5992-TSE",
		#"5998-TSE",
		#"6005-TSE",
		#"6013-TSE",
		#"6023-TSE",
		#"6026-TSE",
		#"6027-TSE",
		#"6028-TSE",
		#"6029-TSE",
		#"6030-TSE",
		#"6031-TSE",
		#"6032-TSE",
		#"6034-TSE",
		#"6035-TSE",
		#"6036-TSE",
		#"6037-TSE",
		#"6038-TSE",
		#"6044-TSE",
		#"6045-TSE",
		#"6046-TSE",
		#"6047-TSE",
		#"6048-TSE",
		#"6049-TSE",
		#"6054-TSE",
		#"6058-TSE",
		#"6064-TSE",
		#"6065-TSE",
		#"6069-TSE",
		#"6072-TSE",
		#"6078-TSE",
		#"6080-TSE",
		#"6081-TSE",
		#"6082-TSE",
		#"6083-TSE",
		#"6086-TSE",
		#"6087-TSE",
		#"6088-TSE",
		#"6090-TSE",
		#"6091-TSE",
		#"6093-TSE",
		#"6094-TSE",
		#"6095-TSE",
		#"6097-TSE",
		#"6099-TSE",
		#"6103-TSE",
		#"6104-TSE",
		#"6118-TSE",
		#"6121-TSE",
		#"6131-TSE",
		#"6134-TSE",
		#"6135-TSE",
		#"6138-TSE",
		#"6140-TSE",
		#"6143-TSE",
		#"6145-TSE",
		#"6146-TSE",
		#"6147-TSE",
		#"6149-TSE",
		#"6155-TSE",
		#"6158-TSE",
		#"6161-TSE",
		#"6164-TSE",
		#"6165-TSE",
		#"6167-TSE",
		#"6171-TSE",
		#"6172-TSE",
		#"6173-TSE",
		#"6176-TSE",
		#"6177-TSE",
		#"6178-TSE",
		#"6180-TSE",
		#"6181-TSE",
		#"6182-TSE",
		#"6183-TSE",
		#"6184-TSE",
		#"6185-TSE",
		#"6186-TSE",
		#"6201-TSE",
		#"6205-TSE",
		#"6217-TSE",
		#"6218-TSE",
		#"6238-TSE",
		#"6239-TSE",
		#"6240-TSE",
		#"6246-TSE",
		#"6247-TSE",
		#"6250-TSE",
		#"6254-TSE",
		#"6255-TSE",
		#"6256-TSE",
		#"6257-TSE",
		#"6258-TSE",
		#"6264-TSE",
		#"6266-TSE",
		#"6268-TSE",
		#"6274-TSE",
		#"6277-TSE",
		#"6282-TSE",
		#"6284-TSE",
		#"6286-TSE",
		#"6287-TSE",
		#"6289-TSE",
		#"6291-TSE",
		#"6294-TSE",
		#"6297-TSE",
		#"6299-TSE",
		#"6300-TSE",
		#"6302-TSE",
		#"6310-TSE",
		#"6315-TSE",
		#"6316-TSE",
		#"6319-TSE",
		#"6324-TSE",
		#"6328-TSE",
		#"6330-TSE",
		#"6332-TSE",
		#"6335-TSE",
		#"6336-TSE",
		#"6339-TSE",
		#"6340-TSE",
		#"6345-TSE",
		#"6349-TSE",
		#"6355-TSE",
		#"6356-TSE",
		#"6358-TSE",
		#"6361-TSE",
		#"6362-TSE",
		#"6366-TSE",
		#"6369-TSE",
		#"6370-TSE",
		#"6373-TSE",
		#"6379-TSE",
		#"6380-TSE",
		#"6382-TSE",
		#"6384-TSE",
		#"6390-TSE",
		#"6393-TSE",
		#"6397-TSE",
		#"6400-TSE",
		#"6402-TSE",
		#"6405-TSE",
		#"6406-TSE",
		#"6409-TSE",
		#"6411-TSE",
		#"6412-TSE",
		#"6413-TSE",
		#"6417-TSE",
		#"6418-TSE",
		#"6419-TSE",
		#"6420-TSE",
		#"6428-TSE",
		#"6432-TSE",
		#"6433-TSE",
		#"6436-TSE",
		#"6440-TSE",
		#"6444-TSE",
		#"6448-TSE",
		#"6454-TSE",
		#"6457-TSE",
		#"6458-TSE",
		#"6459-TSE",
		#"6460-TSE",
		#"6462-TSE",
		#"6463-TSE",
		#"6464-TSE",
		#"6467-TSE",
		#"6470-TSE",
		#"6471-TSE",
		#"6472-TSE",
		#"6474-TSE",
		#"6479-TSE",
		#"6481-TSE",
		#"6485-TSE",
		#"6486-TSE",
		#"6490-TSE",
		#"6492-TSE",
		#"6493-TSE",
		#"6494-TSE",
		#"6501-TSE",
		#"6503-TSE",
		#"6504-TSE",
		#"6505-TSE",
		#"6506-TSE",
		#"6507-TSE",
		#"6508-TSE",
		#"6513-TSE",
		#"6584-TSE",
		#"6586-TSE",
		#"6588-TSE",
		#"6590-TSE",
		#"6592-TSE",
		#"6594-TSE",
		#"6616-TSE",
		#"6617-TSE",
		#"6619-TSE",
		#"6624-TSE",
		#"6626-TSE",
		#"6627-TSE",
		#"6628-TSE",
		#"6630-TSE",
		#"6634-TSE",
		#"6636-TSE",
		#"6637-TSE",
		#"6639-TSE",
		#"6641-TSE",
		#"6643-TSE",
		#"6651-TSE",
		#"6652-TSE",
		#"6654-TSE",
		#"6659-TSE",
		#"6662-TSE",
		#"6664-TSE",
		#"6668-TSE",
		#"6669-TSE",
		#"6670-TSE",
		#"6675-TSE",
		#"6677-TSE",
		#"6678-TSE",
		#"6703-TSE",
		#"6704-TSE",
		#"6707-TSE",
		#"6709-TSE",
		#"6718-TSE",
		#"6721-TSE",
		#"6728-TSE",
		#"6730-TSE",
		#"6734-TSE",
		#"6736-TSE",
		#"6741-TSE",
		#"6742-TSE",
		#"6747-TSE",
		#"6748-TSE",
		#"6751-TSE",
		#"6753-TSE",
		#"6754-TSE",
		#"6755-TSE",
		#"6756-TSE",
		#"6757-TSE",
		#"6758-TSE",
		#"6762-TSE",
		#"6763-TSE",
		#"6768-TSE",
		#"6773-TSE",
		#"6778-TSE",
		#"6779-TSE",
		#"6786-TSE",
		#"6788-TSE",
		#"6789-TSE",
		#"6791-TSE",
		#"6796-TSE",
		#"6803-TSE",
		#"6804-TSE",
		#"6806-TSE",
		#"6807-TSE",
		#"6809-TSE",
		#"6810-TSE",
		#"6814-TSE",
		#"6815-TSE",
		#"6816-TSE",
		#"6817-TSE",
		#"6824-TSE",
		#"6826-TSE",
		#"6832-TSE",
		#"6834-TSE",
		#"6835-TSE",
		#"6837-TSE",
		#"6838-TSE",
		#"6840-TSE",
		#"6844-TSE",
		#"6845-TSE",
		#"6848-TSE",
		#"6849-TSE",
		#"6850-TSE",
		#"6853-TSE",
		#"6855-TSE",
		#"6856-TSE",
		#"6858-TSE",
		#"6859-TSE",
		#"6860-TSE",
		#"6861-TSE",
		#"6863-TSE",
		#"6866-TSE",
		#"6870-TSE",
		#"6871-TSE",
		#"6875-TSE",
		#"6877-TSE",
		#"6879-TSE",
		#"6881-TSE",
		#"6882-TSE",
		#"6889-TSE",
		#"6894-TSE",
		#"6899-TSE",
		#"6902-TSE",
		#"6905-TSE",
		#"6907-TSE",
		#"6908-TSE",
		#"6914-TSE",
		#"6915-TSE",
		#"6916-TSE",
		#"6920-TSE",
		#"6923-TSE",
		#"6924-TSE",
		#"6927-TSE",
		#"6928-TSE",
		#"6930-TSE",
		#"6932-TSE",
		#"6937-TSE",
		#"6938-TSE",
		#"6941-TSE",
		#"6943-TSE",
		#"6944-TSE",
		#"6945-TSE",
		#"6946-TSE",
		#"6947-TSE",
		#"6951-TSE",
		#"6955-TSE",
		#"6960-TSE",
		#"6962-TSE",
		#"6963-TSE",
		#"6965-TSE",
		#"6966-TSE",
		#"6967-TSE",
		#"6969-TSE",
		#"6973-TSE",
		#"6981-TSE",
		#"6982-TSE",
		#"6985-TSE",
		#"6988-TSE",
		#"6989-TSE",
		#"6995-TSE",
		#"6996-TSE",
		#"6997-TSE",
		#"6998-TSE",
		#"6999-TSE",
		#"7004-TSE",
		#"7012-TSE",
		#"7013-TSE",
		#"7102-TSE",
		#"7148-TSE",
		#"7161-TSE",
		#"7162-TSE",
		#"7164-TSE",
		#"7167-TSE",
		#"7169-TSE",
		#"7172-TSE",
		#"7173-TSE",
		#"7180-TSE",
		#"7181-TSE",
		#"7182-TSE",
		#"7183-TSE",
		#"7202-TSE",
		#"7208-TSE",
		#"7211-TSE",
		#"7212-TSE",
		#"7213-TSE",
		#"7214-TSE",
		#"7215-TSE",
		#"7217-TSE",
		#"7218-TSE",
		#"7220-TSE",
		#"7222-TSE",
		#"7224-TSE",
		#"7226-TSE",
		#"7229-TSE",
		#"7236-TSE",
		#"7239-TSE",
		#"7241-TSE",
		#"7242-TSE",
		#"7244-TSE",
		#"7245-TSE",
		#"7247-TSE",
		#"7248-TSE",
		#"7256-TSE",
		#"7260-TSE",
		#"7264-TSE",
		#"7267-TSE",
		#"7271-TSE",
		#"7272-TSE",
		#"7273-TSE",
		#"7274-TSE",
		#"7276-TSE",
		#"7278-TSE",
		#"7279-TSE",
		#"7280-TSE",
		#"7284-TSE",
		#"7291-TSE",
		#"7292-TSE",
		#"7294-TSE",
		#"7296-TSE",
		#"7298-TSE",
		#"7305-TSE",
		#"7309-TSE",
		#"7312-TSE",
		#"7313-TSE",
		#"7404-TSE",
		#"7408-TSE",
		#"7412-TSE",
		#"7416-TSE",
		#"7420-TSE",
		#"7427-TSE",
		#"7433-TSE",
		#"7435-TSE",
		#"7438-TSE",
		#"7442-TSE",
		#"7444-TSE",
		#"7445-TSE",
		#"7451-TSE",
		#"7453-TSE",
		#"7455-TSE",
		#"7456-TSE",
		#"7458-TSE",
		#"7463-TSE",
		#"7466-TSE",
		#"7467-TSE",
		#"7472-TSE",
		#"7475-TSE",
		#"7476-TSE",
		#"7477-TSE",
		#"7481-TSE",
		#"7482-TSE",
		#"7487-TSE",
		#"7494-TSE",
		#"7500-TSE",
		#"7501-TSE",
		#"7502-TSE",
		#"7506-TSE",
		#"7510-TSE",
		#"7512-TSE",
		#"7513-TSE",
		#"7516-TSE",
		#"7517-TSE",
		#"7518-TSE",
		#"7520-TSE",
		#"7522-TSE",
		#"7524-TSE",
		#"7525-TSE",
		#"7527-TSE",
		#"7532-TSE",
		#"7539-TSE",
		#"7544-TSE",
		#"7545-TSE",
		#"7551-TSE",
		#"7552-TSE",
		#"7554-TSE",
		#"7559-TSE",
		#"7570-TSE",
		#"7575-TSE",
		#"7577-TSE",
		#"7587-TSE",
		#"7590-TSE",
		#"7591-TSE",
		#"7594-TSE",
		#"7595-TSE",
		#"7596-TSE",
		#"7599-TSE",
		#"7601-TSE",
		#"7602-TSE",
		#"7603-TSE",
		#"7604-TSE",
		#"7607-TSE",
		#"7608-TSE",
		#"7611-TSE",
		#"7612-TSE",
		#"7613-TSE",
		#"7614-TSE",
		#"7615-TSE",
		#"7616-TSE",
		#"7618-TSE",
		#"7623-TSE",
		#"7636-TSE",
		#"7637-TSE",
		#"7640-TSE",
		#"7643-TSE",
		#"7646-TSE",
		#"7649-TSE",
		#"7702-TSE",
		#"7703-TSE",
		#"7705-TSE",
		#"7709-TSE",
		#"7713-TSE",
		#"7715-TSE",
		#"7716-TSE",
		#"7718-TSE",
		#"7719-TSE",
		#"7722-TSE",
		#"7723-TSE",
		#"7725-TSE",
		#"7727-TSE",
		#"7729-TSE",
		#"7730-TSE",
		#"7733-TSE",
		#"7734-TSE",
		#"7740-TSE",
		#"7743-TSE",
		#"7744-TSE",
		#"7745-TSE",
		#"7748-TSE",
		#"7749-TSE",
		#"7752-TSE",
		#"7758-TSE",
		#"7760-TSE",
		#"7762-TSE",
		#"7771-TSE",
		#"7774-TSE",
		#"7776-TSE",
		#"7779-TSE",
		#"7813-TSE",
		#"7814-TSE",
		#"7816-TSE",
		#"7817-TSE",
		#"7821-TSE",
		#"7823-TSE",
		#"7826-TSE",
		#"7827-TSE",
		#"7831-TSE",
		#"7832-TSE",
		#"7833-TSE",
		#"7834-TSE",
		#"7836-TSE",
		#"7837-TSE",
		#"7839-TSE",
		#"7844-TSE",
		#"7846-TSE",
		#"7847-TSE",
		#"7851-TSE",
		#"7853-TSE",
		#"7855-TSE",
		#"7862-TSE",
		#"7864-TSE",
		#"7865-TSE",
		#"7867-TSE",
		#"7868-TSE",
		#"7871-TSE",
		#"7872-TSE",
		#"7874-TSE",
		#"7879-TSE",
		#"7885-TSE",
		#"7886-TSE",
		#"7888-TSE",
		#"7896-TSE",
		#"7897-TSE",
		#"7898-TSE",
		#"7905-TSE",
		#"7912-TSE",
		#"7914-TSE",
		#"7915-TSE",
		#"7916-TSE",
		#"7917-TSE",
		#"7918-TSE",
		#"7919-TSE",
		#"7920-TSE",
		#"7923-TSE",
		#"7925-TSE",
		#"7927-TSE",
		#"7936-TSE",
		#"7937-TSE",
		#"7938-TSE",
		#"7942-TSE",
		#"7943-TSE",
		#"7945-TSE",
		#"7946-TSE",
		#"7947-TSE",
		#"7951-TSE",
		#"7953-TSE",
		#"7958-TSE",
		#"7961-TSE",
		#"7965-TSE",
		#"7968-TSE",
		#"7970-TSE",
		#"7971-TSE",
		#"7974-TSE",
		#"7975-TSE",
		#"7976-TSE",
		#"7979-TSE",
		#"7981-TSE",
		#"7984-TSE",
		#"7985-TSE",
		#"7987-TSE",
		#"7988-TSE",
		#"7990-TSE",
		#"7991-TSE",
		#"7994-TSE",
		#"7995-TSE",
		#"7999-TSE",
		#"8005-TSE",
		#"8007-TSE",
		#"8011-TSE",
		#"8014-TSE",
		#"8015-TSE",
		#"8016-TSE",
		#"8018-TSE",
		#"8020-TSE",
		#"8022-TSE",
		#"8025-TSE",
		#"8028-TSE",
		#"8029-TSE",
		#"8036-TSE",
		#"8038-TSE",
		#"8044-TSE",
		#"8051-TSE",
		#"8053-TSE",
		#"8056-TSE",
		#"8057-TSE",
		#"8059-TSE",
		#"8060-TSE",
		#"8061-TSE",
		#"8065-TSE",
		#"8066-TSE",
		#"8068-TSE",
		#"8070-TSE",
		#"8074-TSE",
		#"8075-TSE",
		#"8078-TSE",
		#"8081-TSE",
		#"8084-TSE",
		#"8086-TSE",
		#"8087-TSE",
		#"8089-TSE",
		#"8098-TSE",
		#"8101-TSE",
		#"8103-TSE",
		#"8105-TSE",
		#"8113-TSE",
		#"8114-TSE",
		#"8117-TSE",
		#"8118-TSE",
		#"8119-TSE",
		#"8125-TSE",
		#"8127-TSE",
		#"8129-TSE",
		#"8130-TSE",
		#"8131-TSE",
		#"8132-TSE",
		#"8133-TSE",
		#"8135-TSE",
		#"8136-TSE",
		#"8139-TSE",
		#"8140-TSE",
		#"8141-TSE",
		#"8142-TSE",
		#"8143-TSE",
		#"8150-TSE",
		#"8151-TSE",
		#"8152-TSE",
		#"8153-TSE",
		#"8155-TSE",
		#"8157-TSE",
		#"8158-TSE",
		#"8159-TSE",
		#"8168-TSE",
		#"8173-TSE",
		#"8179-TSE",
		#"8181-TSE",
		#"8182-TSE",
		#"8184-TSE",
		#"8186-TSE",
		#"8194-TSE",
		#"8198-TSE",
		#"8200-TSE",
		#"8203-TSE",
		#"8207-TSE",
		#"8214-TSE",
		#"8218-TSE",
		#"8219-TSE",
		#"8225-TSE",
		#"8227-TSE",
		#"8233-TSE",
		#"8237-TSE",
		#"8242-TSE",
		#"8244-TSE",
		#"8245-TSE",
		#"8247-TSE",
		#"8252-TSE",
		#"8255-TSE",
		#"8267-TSE",
		#"8274-TSE",
		#"8275-TSE",
		#"8276-TSE",
		#"8278-TSE",
		#"8281-TSE",
		#"8282-TSE",
		#"8283-TSE",
		#"8287-TSE",
		#"8289-TSE",
		#"8303-TSE",
		#"8304-TSE",
		#"8309-TSE",
		#"8316-TSE",
		#"8324-TSE",
		#"8331-TSE",
		#"8336-TSE",
		#"8338-TSE",
		#"8341-TSE",
		#"8342-TSE",
		#"8343-TSE",
		#"8344-TSE",
		#"8345-TSE",
		#"8346-TSE",
		#"8354-TSE",
		#"8355-TSE",
		#"8356-TSE",
		#"8358-TSE",
		#"8359-TSE",
		#"8362-TSE",
		#"8363-TSE",
		#"8365-TSE",
		#"8366-TSE",
		#"8367-TSE",
		#"8369-TSE",
		#"8374-TSE",
		#"8382-TSE",
		#"8383-TSE",
		#"8385-TSE",
		#"8386-TSE",
		#"8387-TSE",
		#"8396-TSE",
		#"8418-TSE",
		#"8421-TSE",
		#"8423-TSE",
		#"8473-TSE",
		#"8511-TSE",
		#"8518-TSE",
		#"8519-TSE",
		#"8524-TSE",
		#"8527-TSE",
		#"8529-TSE",
		#"8530-TSE",
		#"8537-TSE",
		#"8542-TSE",
		#"8543-TSE",
		#"8545-TSE",
		#"8550-TSE",
		#"8551-TSE",
		#"8563-TSE",
		#"8566-TSE",
		#"8570-TSE",
		#"8584-TSE",
		#"8586-TSE",
		#"8589-TSE",
		#"8591-TSE",
		#"8595-TSE",
		#"8601-TSE",
		#"8613-TSE",
		#"8614-TSE",
		#"8616-TSE",
		#"8617-TSE",
		#"8624-TSE",
		#"8625-TSE",
		#"8628-TSE",
		#"8648-TSE",
		#"8685-TSE",
		#"8697-TSE",
		#"8699-TSE",
		#"8700-TSE",
		#"8703-TSE",
		#"8704-TSE",
		#"8706-TSE",
		#"8707-TSE",
		#"8708-TSE",
		#"8713-TSE",
		#"8714-TSE",
		#"8715-TSE",
		#"8721-TSE",
		#"8729-TSE",
		#"8732-TSE",
		#"8737-TSE",
		#"8739-TSE",
		#"8746-TSE",
		#"8769-TSE",
		#"8771-TSE",
		#"8772-TSE",
		#"8787-TSE",
		#"8793-TSE",
		#"8798-TSE",
		#"8802-TSE",
		#"8806-TSE",
		#"8835-TSE",
		#"8836-TSE",
		#"8840-TSE",
		#"8841-TSE",
		#"8842-TSE",
		#"8844-TSE",
		#"8850-TSE",
		#"8869-TSE",
		#"8870-TSE",
		#"8871-TSE",
		#"8881-TSE",
		#"8885-TSE",
		#"8887-TSE",
		#"8889-TSE",
		#"8890-TSE",
		#"8892-TSE",
		#"8893-TSE",
		#"8897-TSE",
		#"8898-TSE",
		#"8903-TSE",
		#"8905-TSE",
		#"8908-TSE",
		#"8912-TSE",
		#"8914-TSE",
		#"8918-TSE",
		#"8922-TSE",
		#"8925-TSE",
		#"8927-TSE",
		#"8931-TSE",
		#"8933-TSE",
		#"8934-TSE",
		#"8935-TSE",
		#"8938-TSE",
		#"8945-TSE",
		#"8951-TSE",
		#"8953-TSE",
		#"8956-TSE",
		#"8957-TSE",
		#"8958-TSE",
		#"8960-TSE",
		#"8961-TSE",
		#"8966-TSE",
		#"8967-TSE",
		#"8968-TSE",
		#"8976-TSE",
		#"8977-TSE",
		#"8979-TSE",
		#"8984-TSE",
		#"8985-TSE",
		#"8986-TSE",
		#"8999-TSE",
		#"900017613-TSE",
		#"900037250-TSE",
		#"900039616-TSE",
		#"900066758-TSE",
		#"9003-TSE",
		#"9006-TSE",
		#"9007-TSE",
		#"9008-TSE",
		#"9009-TSE",
		#"9010-TSE",
		#"9014-TSE",
		#"9021-TSE",
		#"9022-TSE",
		#"9028-TSE",
		#"9036-TSE",
		#"9039-TSE",
		#"9041-TSE",
		#"9042-TSE",
		#"9045-TSE",
		#"9048-TSE",
		#"9052-TSE",
		#"9055-TSE",
		#"9058-TSE",
		#"9059-TSE",
		#"9062-TSE",
		#"9064-TSE",
		#"9065-TSE",
		#"9066-TSE",
		#"9069-TSE",
		#"9072-TSE",
		#"9074-TSE",
		#"9076-TSE",
		#"9081-TSE",
		#"9101-TSE",
		#"9107-TSE",
		#"9232-TSE",
		#"9233-TSE",
		#"9303-TSE",
		#"9304-TSE",
		#"9305-TSE",
		#"9306-TSE",
		#"9308-TSE",
		#"9310-TSE",
		#"9318-TSE",
		#"9319-TSE",
		#"9322-TSE",
		#"9324-TSE",
		#"9351-TSE",
		#"9353-TSE",
		#"9358-TSE",
		#"9363-TSE",
		#"9364-TSE",
		#"9366-TSE",
		#"9368-TSE",
		#"9369-TSE",
		#"9370-TSE",
		#"9373-TSE",
		#"9377-TSE",
		#"9381-TSE",
		#"9384-TSE",
		#"9386-TSE",
		#"9399-TSE",
		#"9401-TSE",
		#"9404-TSE",
		#"9409-TSE",
		#"9413-TSE",
		#"9414-TSE",
		"9416-TSE",
		"9418-TSE",
		"9419-TSE",
		"9421-TSE",
		"9423-TSE",
		"9424-TSE",
		"9425-TSE",
		"9428-TSE",
		"9435-TSE",
		"9436-TSE",
		"9438-TSE",
		"9441-TSE",
		"9444-TSE",
		"9446-TSE",
		"9467-TSE",
		"9468-TSE",
		"9470-TSE",
		"9475-TSE",
		"9479-TSE",
		"9502-TSE",
		"9506-TSE",
		"9509-TSE",
		"9514-TSE",
		"9517-TSE",
		"9531-TSE",
		"9533-TSE",
		"9534-TSE",
		"9543-TSE",
		"9551-TSE",
		"9603-TSE",
		"9605-TSE",
		"9612-TSE",
		"9616-TSE",
		"9624-TSE",
		"9625-TSE",
		"9627-TSE",
		"9629-TSE",
		"9632-TSE",
		"9633-TSE",
		"9641-TSE",
		"9644-TSE",
		"9651-TSE",
		"9658-TSE",
		"9663-TSE",
		"9671-TSE",
		"9682-TSE",
		"9684-TSE",
		"9687-TSE",
		"9692-TSE",
		"9697-TSE",
		"9699-TSE",
		"9702-TSE",
		"9704-TSE",
		"9706-TSE",
		"9707-TSE",
		"9708-TSE",
		"9709-TSE",
		"9715-TSE",
		"9716-TSE",
		"9717-TSE",
		"9719-TSE",
		"9722-TSE",
		"9726-TSE",
		"9728-TSE",
		"9731-TSE",
		"9735-TSE",
		"9739-TSE",
		"9740-TSE",
		"9743-TSE",
		"9746-TSE",
		"9755-TSE",
		"9757-TSE",
		"9763-TSE",
		"9765-TSE",
		"9768-TSE",
		"9782-TSE",
		"9783-TSE",
		"9790-TSE",
		"9791-TSE",
		"9792-TSE",
		"9793-TSE",
		"9797-TSE",
		"9810-TSE",
		"9812-TSE",
		"9816-TSE",
		"9823-TSE",
		"9828-TSE",
		"9831-TSE",
		"9836-TSE",
		"9837-TSE",
		"9843-TSE",
		"9846-TSE",
		"9853-TSE",
		"9854-TSE",
		"9856-TSE",
		"9861-TSE",
		"9869-TSE",
		"9876-TSE",
		"9882-TSE",
		"9885-TSE",
		"9887-TSE",
		"9888-TSE",
		"9896-TSE",
		"9899-TSE",
		"9902-TSE",
		"9903-TSE",
		"9904-TSE",
		"9906-TSE",
		"9908-TSE",
		"9913-TSE",
		"9919-TSE",
		"9930-TSE",
		"9932-TSE",
		"9934-TSE",
		"9948-TSE",
		"9950-TSE",
		"9956-TSE",
		"9959-TSE",
		"9960-TSE",
		"9962-TSE",
		"9966-TSE",
		"9969-TSE",
		"9972-TSE",
		"9973-TSE",
		"9974-TSE",
		"9980-TSE",
		"9983-TSE",
		"9986-TSE",
		"9987-TSE",
		"9989-TSE",
		"9991-TSE",
		"9994-TSE",
		"9995-TSE",
		"F3440-TSE",
		"F6076-TSE",
		"F9035-TSE",
		"F9942-TSE",
		"S2928-TSE"    
]
#db_list = ["DWConfiguration"] if os_util.get_user() == "operator" else ["eSignal_201603", "eSignal_201602", "eSignal_201601", "eSignal_201512"]
db_list = ['eSignal']
schema = "dbo"
#table = "configuration_node" if os_util.get_user() == "operator" else "TOCOM"
table = 'tse_orc'
#evt_time_list = ["2015-11-27 12:49:34.000", "2015-11-28 12:49:34.000"] if os_util.get_user() == "operator" else ["2016-04-01 01:13:16.000", "2016-04-01 01:13:16.000"]
num_of_obs = 100
interval_in_sec = 300

def calc_all_ptr(ptr):

    import pandas as pd
    
    for product in product_list:

        c = pd.DataFrame(columns = ['duration','uratio', 'ba', 'product', 'ptr', 'dt'])

        for db_name in db_list:

            for ba in ['a','b']:

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
                        sqlstr = tsql_bevt_ptr1(db_name, table, product)
                    elif ptr == 2:
                        sqlstr = tsql_bevt_ptr2(db_name, table, product)
                    elif ptr == 3:
                        sqlstr = tsql_bevt_ptr3(db_name, table, product)
                    elif ptr == 4:
                        sqlstr = tsql_bevt_ptr4(db_name, table, product)
                    elif ptr == 5:
                        sqlstr = tsql_bevt_ptr5(db_name, table, product)
                    elif ptr == 6:
                        sqlstr = tsql_bevt_ptr6(db_name, table, product)
                    elif ptr == 7:
                        sqlstr = tsql_bevt_ptr7(db_name, table, product)
                    elif ptr == 8:
                        sqlstr = tsql_bevt_ptr8(db_name, table, product)
                if ba == 'a':
                    if ptr == 1:
                        sqlstr = tsql_aevt_ptr1(db_name, table, product)
                    elif ptr == 2:
                        sqlstr = tsql_aevt_ptr2(db_name, table, product)
                    elif ptr == 3:
                        sqlstr = tsql_aevt_ptr3(db_name, table, product)
                    elif ptr == 4:
                        sqlstr = tsql_aevt_ptr4(db_name, table, product)
                    elif ptr == 5:
                        sqlstr = tsql_aevt_ptr5(db_name, table, product)
                    elif ptr == 6:
                        sqlstr = tsql_aevt_ptr6(db_name, table, product)
                    elif ptr == 7:
                        sqlstr = tsql_aevt_ptr7(db_name, table, product)
                    elif ptr == 8:
                        sqlstr = tsql_aevt_ptr8(db_name, table, product)

                # make it a one line statement
                hiveql = sqlstr.replace('\n', ' ').replace('\r', '').replace('     ',' ').replace('     ',' ').replace('     ',' ').replace('     ',' ')
                # some tricks needed 
                hiveql = hiveql.replace("'","'\\''")
                #print(hiveql)
                win_shell = "plink -pw M9rgan?? p000505@itubuntu02  sudo su - hive -c 'hive -e \\\"{0}\\\"'".format(hiveql)

                print(win_shell,'/n') # to check what been passed to win_shell
                result = check_output(win_shell, shell=True).decode('utf-8').strip()  # returns a byte object.
                if result == '':
                    continue
                #print(result)
                # convert result list into list of list of string
                res_list = [x.split('\t') for x in result.split('\n')]
                # convert this list into a DataFrame
                res_df = pd.DataFrame(res_list[0:]) #, columns=["duration", "chg",	"act", "val", "dt", "mp", "id", "prevact", "prevdt", "prevchg"])                
                res_df = res_df.iloc[:,0:10]
                res_df.columns = ["duration", "chg", "act", "val", "dt", "id", "mp", "prevact", "prevdt", "prevchg"]
                #res_df=res_df.rename(columns = {0, 'duration'})
                #res_df=res_df.rename(columns = {'1':'chg'})
                #res_df=res_df.rename(columns = {'2':'act'})
                #res_df=res_df.rename(columns = {'3':'val'})
                #res_df=res_df.rename(columns = {'4':'dt'})
                #res_df=res_df.rename(columns = {'5':'mp'})
                #res_df=res_df.rename(columns = {'6':'id'})
                evt_time_list = [dpu.str_to_datetime(x) for x in res_df['dt'].tolist()]
                dur_list = res_df['duration'].tolist()
                mp_list = res_df['mp'].tolist()

                #data_frame = db_util.make_dataFrame(db_cursor)
                # summary
                # http://sinhrks.hatenablog.com/entry/2014/10/13/005327
                #daily_evt_freq = data_frame.groupby('Date')['act'].count()
                #print(daily_evt_freq)
                #print(data_frame)
                #evt_time_list = data_frame['dt'].astype('str').tolist()
                #dur_list = data_frame['Duration'].astype(int).tolist()

                # post
                if len(res_list) == 0:
                    print("skipped {0} {1} {2} {3}".format(db_name,product,ba,ptr))
                    continue

                # get all the micro price list after the first event time. this is to save throwing similar queries repeatedly.
                sqlstr = tsql_microprice(db_name, table, product, evt_time_list[0])
                # make it a one line statement
                hiveql = sqlstr.replace('\n', ' ').replace('\r', '').replace('     ',' ').replace('     ',' ').replace('     ',' ').replace('     ',' ')
                # some tricks needed 
                hiveql = hiveql.replace("'","'\\''")
                print(hiveql)
                win_shell = "plink -pw M9rgan?? p000505@itubuntu02 sudo su - hive -c 'hive -e \\\"{0}\\\"'".format(hiveql)

                print(win_shell,'/n') # to check what been passed to win_shell
                result = check_output(win_shell, shell=True).decode('utf-8').strip()  # returns a byte object.
                if result == '':
                    continue
                print(result)

                # convert result list into list of list of string
                res_list = [x.split('\t') for x in result.split('\n')]
                # convert this list into a DataFrame
                res_df = pd.DataFrame(res_list[0:], columns=["dt", "act", "val", "mp"])

                post_evt_list = [dpu.str_to_datetime(x) for x in res_df['dt'].tolist()]
                post_mp_list = [float(x) for x in res_df['mp'].tolist()]

                #db_cursor  = db_util.mssql_exec(server_instance, login_user, login_pass, db_name, sqlstr)
                ##df = db_util.make_dataFrame(db_cursor)
                #fetchedall = db_cursor.fetchall()
                #post_evt_list = db_util.get_column(db_cursor, 'dt', fetchedall)
                #post_mp_list = db_util.get_column(db_cursor, 'MicroPrice', fetchedall)
                
                #for i, evt_time in enumerate(post_evt_list):
                #    print(i, ":",  evt_time)
    
                start = time.time()
                cep = CppStats.postEvtStat()
                cep.set_post_evts(post_evt_list, post_mp_list)

                ratio = [cep.get_post_evt_stat(evt_time, float(benchmark), post_mp_list, interval_in_sec) for evt_time, benchmark in zip(evt_time_list, mp_list)]
                end = time.time()
                print("exec post evt takes " , end - start," sec")
                #dur = [tpl[0] for tpl in a]
                #ratio = [tpl[1] for tpl in a]
                d = pd.DataFrame()
                d['dt'] = evt_time_list[:len(ratio)]
                d['product'] = [product] * len(ratio)
                d['ptr'] = [ptr] * len(ratio)
                d['ba'] = [ba] * len(ratio)
                d['duration'] = dur_list
                d['uratio'] = ratio
                c = c.append(d)
        
        with open('./{0}_evt_ptr_{1}.csv'.format(product, db_name), 'a') as f:
            c.to_csv(f, index = False)        

for pattern in [1, 2, 3, 4, 5, 6, 7, 8]:
    calc_all_ptr(pattern)

## ref:　http://stackoverflow.com/questions/11968689/python-multithreading-wait-till-all-threads-finished
#import threading 
#m_thread1 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(1,))
#m_thread2 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(2,))
#m_thread3 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(3,))
#m_thread4 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(4,))
#m_thread5 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(5,))
#m_thread6 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(6,))
#m_thread7 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(7,))
#m_thread8 = threading.Thread(target=calc_all_ptr, name="test_thread", args=(8,))

#m_thread1.start()
#m_thread2.start()
#m_thread3.start()
#m_thread4.start()
#m_thread5.start()
#m_thread6.start()
#m_thread7.start()
#m_thread8.start()

#m_thread1.join()
#m_thread2.join()
#m_thread3.join()
#m_thread4.join()
#m_thread5.join()
#m_thread6.join()
#m_thread7.join()
#m_thread8.join()

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