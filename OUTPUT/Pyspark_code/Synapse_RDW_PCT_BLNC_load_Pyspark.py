import sys
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
g = Github("d4d0b8faff83468f501a42da4942267b9f565a29")
repo = g.get_user().get_repo( "Testing" )
files_and_dirs = [fd for fd in repo.get_dir_contents('/')]
fileDataList=[]
contents = repo.get_contents("resources")
while len(contents)>0:
    file_content = contents.pop(0)
    if file_content.type=='dir':
        contents.extend(repo.get_contents(file_content.path))
    else :
        if file_content.name=="properties_1.ini":
            cp.readfp(StringIO(file_content.decoded_content.decode()))



database =cp.get('SQLSERVERDBConnection', 'database')
user = cp.get('SQLSERVERDBConnection', 'user')
password = cp.get('SQLSERVERDBConnection', 'password')
driver = cp.get('SQLSERVERDBConnection', 'driver')
server = cp.get('SQLSERVERDBConnection', 'server')
connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password}')


query4 = "SELECT GETQUERYBAND()"
cursor = connection.cursor()
cursor.execute(query4)
for row in cursor:
    print(row)

query8 = "DELETE FROM $TB_RDW_PCT_BLNC WHERE LOAD_LOG_KEY IN ( SELECT ESI_LOAD_LOG_KEY FROM $TGT_LOAD_LOG_LKUP WHERE XTRCT_LOAD_LOG_KEY IN ( SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOG WHERE WORK_FLOW_NM='$WORK_FLOW_NM' AND LOAD_END_DTM = ( SELECT MAX(LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE WORK_FLOW_NM='$WORK_FLOW_NM' AND PBLSH_IND='N' ) AND PBLSH_IND='N' ) ) AND UPPER(SRC_SYSTM) = '$SRC_SYS_NM'"
cursor = connection.cursor()
cursor.execute(query8)
connection.commit()

query9 = "INSERT INTO $TB_RDW_PCT_BLNC ( SRC_SYSTM, LOAD_LOG_KEY, PBLSH_DTM, ACCNT_NBR, JRNL_DT, EXP_AMT, ADM_AMT, CDH_AMT, ACCT_TOT_AMT, ACCT_CNT )SELECT '$SRC_SYS_NM' AS SRC_SYSTM_CODE, BAL_QRY.LOAD_LOG_KEY, CAST( CONVERT(DATE, GETDATE()) AS DATE FORMAT 'YYYY BAL_QRY.ACCT_NBR, BAL_QRY.GL_POST_DT, BAL_QRY.EXPENSE_AMT, BAL_QRY.ADM_AMT, BAL_QRY.CDHP_AMT, BAL_QRY.SUB_TOTAL, BAL_QRY.ACCT_CNT FROM ( SELECT IN_QUERY.LOAD_LOG_KEY, IN_QUERY.ACCT_NBR, IN_QUERY.GL_POST_DT, COALESCE(SUM (IN_QUERY.EXPENSE_AMT),0) AS EXPENSE_AMT, COALESCE(SUM (IN_QUERY.ADM_AMT),0) AS ADM_AMT, COALESCE(SUM (IN_QUERY.CDHP_AMT),0) AS CDHP_AMT, COALESCE(SUM (IN_QUERY.EXPENSE_AMT),0) + COALESCE(SUM (IN_QUERY.ADM_AMT),0) + COALESCE(SUM (IN_QUERY.CDHP_AMT),0) AS SUB_TOTAL, SUM (IN_QUERY.ACCT_CNT) AS ACCT_CNT FROM ( SELECT CLM_MSTR_LOAD_LOG_KEY AS LOAD_LOG_KEY, GL_ACCNT_NBR AS ACCT_NBR, GL_POST_DT, SUM (PLN_BAL CAST (0 AS DECIMAL (16,2)) AS ADM_AMT, CAST (0 AS DECIMAL (16,2)) AS CDHP_AMT, COUNT (GL_ACCNT_NBR) AS ACCT_CNT FROM $PCT_NM $PCT_NM GROUP BY GL_ACCNT_NBR, GL_POST_DT, CLM_MSTR_LOAD_LOG_KEY WHERE UPPER($PCT_NM.DLY_WKLY) = 'W' AND $PCT_NM.GL_ACCNT_NBR NOT IN ('UNK', 'NA', '') AND UPPER($PCT_NM.SRC_SYSTM) = '$SRC_SYS_NM' UNION SELECT CLM_MSTR_LOAD_LOG_KEY AS LOAD_LOG_KEY, GL_ACCNT_NBR_ADM AS ACCT_NBR, GL_POST_DT, CAST (0 AS DECIMAL (16,2)) AS EXPENSE_AMT, SUM (CAE) AS ADM_AMT, CAST (0 AS DECIMAL (16,2)) AS CDHP_AMT, COUNT (GL_ACCNT_NBR_ADM) AS ACCT_CNT FROM $PCT_NM $PCT_NM GROUP BY GL_ACCNT_NBR_ADM, GL_POST_DT, CLM_MSTR_LOAD_LOG_KEY WHERE UPPER($PCT_NM.DLY_WKLY) = 'W' AND $PCT_NM.GL_ACCNT_NBR_ADM NOT IN ('UNK', 'NA', '') AND UPPER($PCT_NM.SRC_SYSTM) = '$SRC_SYS_NM' UNION SELECT CLM_MSTR_LOAD_LOG_KEY AS LOAD_LOG_KEY, GL_ACCNT_NBR_CDH AS ACCT_NBR, GL_POST_DT, CAST (0 AS DECIMAL (16,2)) AS EXPENSE_AMT, CAST (0 AS DECIMAL (16,2)) AS ADM_AMT, SUM (SPNDG_ACNT_APLD_AMT) AS CDHP_AMT, COUNT (GL_ACCNT_NBR_CDH) AS ACCT_CNT FROM $PCT_NM $PCT_NM GROUP BY GL_ACCNT_NBR_CDH, GL_POST_DT, CLM_MSTR_LOAD_LOG_KEY WHERE UPPER($PCT_NM.DLY_WKLY) = 'W' AND $PCT_NM.GL_ACCNT_NBR_CDH NOT IN ('UNK', 'NA', '') AND UPPER($PCT_NM.SRC_SYSTM) = '$SRC_SYS_NM' ) AS IN_QUERY GROUP BY IN_QUERY.ACCT_NBR, IN_QUERY.GL_POST_DT, IN_QUERY.LOAD_LOG_KEY ) AS BAL_QRY"
cursor = connection.cursor()
cursor.execute(query9)
connection.commit()

