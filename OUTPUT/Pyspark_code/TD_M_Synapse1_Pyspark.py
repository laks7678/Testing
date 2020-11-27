import sys
import os
import logging
import configparser
from github import Github
import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import pyodbc
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col
from _io import StringIO

spark=SparkSession.builder.appName("Building Pyspark code with Synapse statements embedded").getOrCreate()
sc=spark.sparkContext


cp = configparser.ConfigParser()
g = Github(os.environ.get('GITHUB_TOKEN'))
repo = g.get_user().get_repo( 'Testing' )
files_and_dirs = [fd for fd in repo.get_dir_contents('/')]
fileDataList=[]
contents = repo.get_contents('resources')
while len(contents)>0:
    file_content = contents.pop(0)
    if file_content.type=='dir':
        contents.extend(repo.get_contents(file_content.path))
    else :
        if file_content.name=='properties_1.ini':
            cp.readfp(StringIO(file_content.decoded_content.decode()))


database =cp.get('SQLSERVERDBConnection', 'database')
user = cp.get('SQLSERVERDBConnection', 'user')
password = cp.get('SQLSERVERDBConnection', 'password')
driver = cp.get('SQLSERVERDBConnection', 'driver')
server = cp.get('SQLSERVERDBConnection', 'server')
connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password}')


query2 = "SELECT COUNT(* )FROM DBC.TABLESV WHERE DATABASENAME = 'IDEA_TEST_PHASE_RPT'AND TABLENAME = 'IDEA_T_SEM_COMPLETE_STAS'HAVING COUNT(*) > 0"
cursor = connection.cursor()
cursor.execute(query2)
for row in cursor:
    print(row)

query4 = "DELETE IDEA_TEST_PHASE_TRNS.IDEA_T_SEM_ACCOUNT_DETAILS ALL"
cursor = connection.cursor()
cursor.execute(query4)
connection.commit()

query6 = " INSERT IDEA_TEST_PHASE_TGT ABC_T_TGT_BATCH_LOG ( BATCH_ID ,PROCESS_NM,PROCESS_TYPE,BATCH_START_DTTM,BATCH_END_DTTM,BATCH_STATUS,USER_NM,SESSION_ID ) VALUES (:VAR_BATCH_ID ,'IDEA_M_TGT_ACCOUNT','BTEQ' ,CONVERT(DATETIME, GETDATE())(0),NULL, 'STARTED',USER, )"
cursor = connection.cursor()
cursor.execute(query6)
connection.commit()

query7 = " UPDATE IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNTSET ACCOUNT_CLOSE_DTTM = CONVERT(DATETIME, GETDATE())(0) , ETL_UPD_DTTM =CURRENT_TIMESTAMP(6) FROM IDEA_TEST_PHASE_TRNS ABC_T_TRF_ACCOUNT TRFWHERE IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.ACCOUNT_CLOSE_DTTM= '9999AND IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.ACCOUNT_ID =TRF.ACCOUNT_IDAND ( COALESCE(IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.SRC_ACCOUNT_NO,'') <> COALESCE(TRF.SRC_ACCOUNT_NO,'') OR COALESCE(IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.ACCOUNT_TYPE_CD,'') <> COALESCE(TRF.ACCOUNT_TYPE_CD,'') OR COALESCE(IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.ACCOUNT_SOURCE_CD,'UNKNOWN') <> COALESCE(TRF.ACCOUNT_SOURCE_CD,'UNKNOWN')OR COALESCE(IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.ACCOUNT_CURRENT_STATUS_TYPE_CD,'') <> COALESCE(TRF.ACCOUNT_CURRENT_STATUS_TYPE_CD,'') OR COALESCE(IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.DISPOSITION_CD,'') <> COALESCE(TRF.DISPOSITION_CD,'') OR COALESCE(IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.ACCOUNT_NAME,'') <> COALESCE(TRF.ACCOUNT_NAME,'') OR COALESCE(IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT.ACCOUNT_DESC_TXT,'') <> COALESCE(TRF.ACCOUNT_DESC_TXT,'') ) "
cursor = connection.cursor()
cursor.execute(query7)
connection.commit()

query8 = " INSERT IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT(ACCOUNT_ID ,SRC_ACCOUNT_NO ,ACCOUNT_TYPE_CD ,ACCOUNT_SOURCE_CD ,ACCOUNT_CURRENT_STATUS_TYPE_CD,DISPOSITION_CD ,ACCOUNT_BAL_AMT ,ACCOUNT_OPEN_DTTM ,ACCOUNT_CLOSE_DTTM ,ACCOUNT_NAME ,ACCOUNT_DESC_TXT ,ETL_INS_DTTM ,ETL_UPD_DTTM ,BATCH_ID )SELECT TRF.ACCOUNT_ID ,TRF.SRC_ACCOUNT_NO ,TRF.ACCOUNT_TYPE_CD ,TRF.ACCOUNT_SOURCE_CD ,TRF.ACCOUNT_CURRENT_STATUS_TYPE_CD,TRF.DISPOSITION_CD ,TRF.ACCOUNT_BAL_AMT ,TRF.ACCOUNT_OPEN_DTTM ,TRF.ACCOUNT_CLOSE_DTTM ,TRF.ACCOUNT_NAME ,TRF.ACCOUNT_DESC_TXT ,TRF.ETL_INS_DTTM ,TRF.ETL_UPD_DTTM ,:VAR_BATCH_ID FROM IDEA_TEST_PHASE_TRNS.ABC_T_TRF_ACCOUNT TRFLEFT JOIN IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT TGTON TRF.ACCOUNT_ID = TGT.ACCOUNT_IDAND TGT.ACCOUNT_CLOSE_DTTM= '9999WHERE TGT.ACCOUNT_ID IS NULL "
cursor = connection.cursor()
cursor.execute(query8)
connection.commit()

query9 = " UPDATE IDEA_TEST_PHASE_TGT.ABC_T_TGT_BATCH_LOG SET BATCH_STATUS = 'COMPLETED' , BATCH_END_DTTM = CONVERT(DATETIME, GETDATE())(0)WHERE IDEA_TEST_PHASE_TGT.ABC_T_TGT_BATCH_LOG.BATCH_ID =:VAR_BATCH_IDAND IDEA_TEST_PHASE_TGT.ABC_T_TGT_BATCH_LOG.PROCESS_NM ='IDEA_M_TGT_ACCOUNT'AND IDEA_TEST_PHASE_TGT.ABC_T_TGT_BATCH_LOG.SESSION_ID =SESSIONAND CAST(IDEA_TEST_PHASE_TGT.ABC_T_TGT_BATCH_LOG.BATCH_START_DTTM AS DATE ) =CURRENT_DATEAND IDEA_TEST_PHASE_TGT.ABC_T_TGT_BATCH_LOG.BATCH_STATUS <> 'COMPLETED'"
cursor = connection.cursor()
cursor.execute(query9)
connection.commit()

query11 = "CREATE TABLE #ABC_T_TGT_ACCOUNT_SUMMERY_TEMP ( ACCOUNT_ID BIGINT NOT NULL(18,4) NOT NULL, ACCOUNT_TYPE_CD CHAR(15) NOT NULL, ACCOUNT_SOURCE_CD CHAR(10) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC, ACCOUNT_CURRENT_STATUS_TYPE_CD CHAR(20) , DISPOSITION_CD CHAR(1) , ACCOUNT_BAL_AMT DECIMAL(38,8), ACCOUNT_OPEN_DTTM DATETIME2(0), ACCOUNT_CLOSE_DTTM DATETIME2(4), ACCOUNT_NAME NVARCHAR(1000) , ACCOUNT_DESC_TXT NVARCHAR(4000) , ETL_INS_DTTM DATETIME2(6), ETL_UPD_DTTM DATETIME2(6), BATCH_ID DECIMAL(18,0) NOT NULL)DISTRIBUTION = HASH ( ACCOUNT_ID )"
cursor = connection.cursor()
cursor.execute(query11)
connection.commit()

query12 = "DELETE FROM ABC_T_TGT_ACCOUNT_SUMMERY_TEMP "
cursor = connection.cursor()
cursor.execute(query12)
connection.commit()

query13 = "INSERT INTO ABC_T_TGT_ACCOUNT_SUMMERY_TEMPSELECT * FROM IDEA_TEST_PHASE_TGT.ABC_T_TGT_ACCOUNT_SUMMERY "
cursor = connection.cursor()
cursor.execute(query13)
connection.commit()

