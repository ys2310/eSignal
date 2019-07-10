class evExtraction(object):
    """description of class"""

import sys
sys.path.append('C:/Users/steve/Documents/Visual Studio 2015/Projects/PythonStats/x64/Debug/')
sys.path.append('../PythonUtility/')

import DataTypeUtility as dt_util
import DbUtility as db_util

def tsql_bevt_ptr1(_db_name , _schema, _table):
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
			    LAG(ActType,1,NULL) OVER(ORDER BY AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY AutoId) AS PreviousChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]
					    ,[AutoId]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType = 'TQ.↓' ) OR ([ActType] = 'BP' and Chg > 0)
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE ActType <> PreviousAct and ActType = 'TQ.↓' and Duration > 1
    """.format(_db_name , _schema, _table)
    return sqlstr

def tsql_bevt_ptr2(_db_name , _schema, _table):
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
			    LAG(ActType,1,NULL) OVER(ORDER BY AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY AutoId) AS PreviousChg,
			    LAG(ActType,2,NULL) OVER(ORDER BY AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]
					    ,[AutoId]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType = 'TP.↓' ) OR ([ActType] = 'BP')
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'TP.↓' AND (PreviousAct = 'BP' and PreviousChg < 0) AND (ActType = 'BP' and Chg > 0) AND Duration > 1
    """.format(_db_name , _schema, _table)
    return sqlstr

def tsql_bevt_ptr3(_db_name , _schema, _table):
    sqlstr = """
    
    """
    return sqlstr

def tsql_bevt_ptr4(_db_name , _schema, _table):
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
			    LAG(ActType,1,NULL) OVER(ORDER BY AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY AutoId) AS PreviousValue,
			    LAG(ActType,2,NULL) OVER(ORDER BY AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]
					    ,[Value]
					    ,[AutoId]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType LIKE 'TP.%' ) OR ([ActType] = 'BP')
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'TP.↓' AND (PreviousAct = 'BP' and PreviousChg < 0) AND ActType = 'TP.↑' AND Duration > 1
    """.format(_db_name , _schema, _table)
    return sqlstr

def tsql_bevt_ptr5(_db_name , _schema, _table):
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
			    LAG(ActType,1,NULL) OVER(ORDER BY AutoId) AS PreviousAct,	
			    LAG(Datetime,1,NULL) OVER(ORDER BY AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY AutoId) AS PreviousChg	
			    FROM
			    (
				    SELECT TOP 1000
					    [Chg]
					    ,[ActType]      
					    ,[DateTime]
								    ,[AutoId]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType = 'AP' and Chg < 0 ) OR ([ActType] = 'BP' and Chg < 0)
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE ActType <> PreviousAct and ActType = 'AP' and Duration > 1
    """.format(_db_name , _schema, _table)
    return sqlstr

def tsql_bevt_ptr6(_db_name , _schema, _table):
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
			    LAG(ActType,1,NULL) OVER(ORDER BY AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY AutoId) AS PreviousValue
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]
					    ,[AutoId]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType = 'TQ.↑' )
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE Duration < 10 and (Value + PreviousValue > 200)
    """.format(_db_name , _schema, _table)
    return sqlstr

def tsql_bevt_ptr7(_db_name , _schema, _table):
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
			    LAG(ActType,1,NULL) OVER(ORDER BY AutoId) AS PreviousAct,
			    LAG(Datetime,1,NULL) OVER(ORDER BY AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY AutoId) AS PreviousChg,
			    LAG(Value,1,NULL) OVER(ORDER BY AutoId) AS PreviousValue,
			    LAG(ActType,2,NULL) OVER(ORDER BY AutoId) AS PPAct,
			    LAG(Datetime,2,NULL) OVER(ORDER BY AutoId) AS PPDatetime,
			    LAG(Chg,2,NULL) OVER(ORDER BY AutoId) AS PPChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[DateTime]
					    ,[Value]
					    ,[AutoId]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType = 'AP') OR ([ActType] = 'BP' and Chg < 0)
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PPAct = 'BP' AND (PreviousAct = 'AP' and PreviousChg < 0) AND (ActType = 'AP' and Chg > 0) AND Duration > 1
    """.format(_db_name , _schema, _table)
    return sqlstr

def tsql_bevt_ptr8(_db_name , _schema, _table):
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
			    LAG(ActType,1,NULL) OVER(ORDER BY AutoId) AS PreviousAct,
			    LAG(Value,1,NULL) OVER(ORDER BY AutoId) AS PreviousValue,
			    LAG(Datetime,1,NULL) OVER(ORDER BY AutoId) AS PreviousDatetime,
			    LAG(Chg,1,NULL) OVER(ORDER BY AutoId) AS PreviousChg
			    FROM
			    (
				    SELECT
					    [Chg]
					    ,[ActType]
					    ,[Value]
					    ,[DateTime]
					    ,[AutoId]
				    FROM [{0}].[{1}].[{2}]
				    WHERE (ActType = 'TQ.↓' ) OR ([ActType] = 'AP' and Chg > 0)
			    ) AS NEXT1
		    ) AS NEST2
	    ) AS NEST3
    ) AS NEST4
    WHERE PreviousAct = 'TQ.↓' and ActType = 'AP' and Duration > 1
    """.format(_db_name , _schema, _table)
    return sqlstr

def tsql_leadtime(_db_name, _schema, _table, _product, _evt_time, _num_of_obs=100):
    sqlstr = """
    SELECT TOP 1 
	LEAD(DateTime,100-1,NULL) OVER(ORDER BY DateTime) AS Next100TradeTimestamp 
	FROM [DWConfiguration].[dbo].[configuration_node]
	WHERE ActType LIKE 'TP%' and Symbol = '1321-TSE'    
    """.format(_num_of_obs, _db_name, _schema, _table, _product, _evt_time)
    return sqlstr
 #   SELECT TOP 1 
	#LEAD(DateTime,{0}-1,NULL) OVER(ORDER BY DateTime) AS Next100TradeTimestamp 
	#FROM [{1}].[{2}].[{3}] 
	#WHERE ActType LIKE 'TP%' and Symbol = {4} and DateTime >= CONVERT(DATETIME,{5})

def tsql_post_evt_stat(_db_name, _schema, _table, _product, _benchprice, _evt_time, _lead_time, _num_of_obs):
    sqlstr = """
    SELECT 
	COUNT(CASE WHEN Value > {4} THEN 1 END) / ({7} * 1.0) AS UpProb,
	COUNT(CASE WHEN Value < {4} THEN 1 END) / ({7} * 1.0) AS DnProb
	FROM [{0}].[{1}].[{2}] 
	WHERE Symbol = {3} and DateTime Between CONVERT(DATETIME,{5}) and {6} and ActType LIKE 'TP%'
    """.format(_db_name, _schema, _table, _product, _benchrice, _evt_time, _lead_time, _num_of_obs)
    return sqlstr

# do some test
#selected_cols = ["Date", "Time", "ActType"]
#sqlstr = tsql_bevt_ptr1("DWConfiguration","dbo","configuration_node")
#db_cursor = db_util.mssql_exec("P11013", "sa", "Bigdata01", "DWConfiguration", sqlstr)
#db_util.show_rows(db_cursor)

# post stat
sqlstr = tsql_leadtime("DWConfiguration", "dbo", "configuration_node", "1321-TSE", "2015-11-27 12:50:22")
db_cursor = db_util.mssql_exec("P11013", "sa", "Bigdata01", "DWConfiguration", sqlstr)
db_util.show_rows(db_cursor)

