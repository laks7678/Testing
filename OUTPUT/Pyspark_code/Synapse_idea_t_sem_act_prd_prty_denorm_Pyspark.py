import sys
import os
import logging
#Importing Config parser object to read property files
import configparser
#Connecting github using pygithub package
from github import Github
import findspark
findspark.init()
findspark.find()
#Importing spark packages
import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
#Importing SparkSession
from pyspark.sql import SparkSession
import pyodbc
import pandas as pd
#Importing spark sql packages
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col
from _io import StringIO



cp = configparser.ConfigParser()
#Get the github token from environment variables to access github repositoryg = Github(os.environ.get('GITHUB_TOKEN'))
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

spark=SparkSession.builder.appName(cp.get('PySparkProp', 'appName')).getOrCreate()
sc=spark.sparkContext
database =cp.get('SQLSERVERDBConnection', 'database')
user = cp.get('SQLSERVERDBConnection', 'user')
password = cp.get('SQLSERVERDBConnection', 'password')
driver = cp.get('SQLSERVERDBConnection', 'driver')
server = cp.get('SQLSERVERDBConnection', 'server')
connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password}')


df0 = "SELECT COUNT(1) FROM (SELECT ACP.ACCOUNT_ID, ACP.ACCOUNT_TYPE_CD,ACCT_PROD_START_DTTM,PRODUCT_ID FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT ACC JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PRODUCT ACP ON ACC.ACCOUNT_ID = ACP.ACCOUNT_ID AND CAST(ACP.ACCT_PROD_START_DTTM AS DATE ) BETWEEN ACC.ACCOUNT_OPEN_DTTM AND ACC.ACCOUNT_CLOSE_DTTM GROUP BY ACP.ACCOUNT_ID, ACP.ACCOUNT_TYPE_CD,ACCT_PROD_START_DTTM,PRODUCT_ID ) DTINNER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PARTY AP ON DT.ACCOUNT_ID = AP.ACCOUNT_IDAND CAST(DT.ACCT_PROD_START_DTTM AS DATE ) BETWEEN AP.ACCOUNT_PARTY_START_DT AND AP.ACCOUNT_PARTY_END_DT WHERE NOT EXISTS (SELECT 1 FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PRODUCT PR WHERE DT.PRODUCT_ID = PR.PRODUCT_ID AND CAST(PRODUCT_END_DT AS DATE )= '1900 )AND AP.PARTY_ID IN (SELECT PARTY_ID FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY P WHERE P.PARTY_END_DT > CONVERT(DATE, GETDATE()) GROUP BY PARTY_ID )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

df1 = "SELECT EXECUTION_DT,COUNT(*) FROM (SELECT ACCOUNT_ID ,EXECUTION_DT, SUM(LOAN_AMT) AS ACCUMULATED_LOAN_AMT FROM (SELECT LON. ACCOUNT_ID , LON.LOAN_AMT, EXECUTION_DT FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_LOAN_TERM_ACCT LON CROSS JOIN (SELECT CONVERT(DATE, GETDATE()) AS EXECUTION_DT FROM (SELECT 1 A ) TEMP ) TEMP_1 WHERE LOAN_AMT > 0 AND ACCOUNT_ID IN (SELECT ACCOUNT_ID FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_GL_BALANCE ) )DTGROUP BY ACCOUNT_ID,EXECUTION_DT ) DTRIGHT OUTER JOIN (SELECT ACCOUNT_ID , SUM(CASE WHEN TRANS_TYPE_CD ='D' THEN ,SUM(CASE WHEN TRANS_TYPE_CD ='D' THEN ,MAX(OPENING_BAL_AMT) MAX_OPENING_BAL_AMT,AVG(CLOSING_BAL_AMT) AS AVG_CLOSING_BAL_AMT,MIN(OPENING_BAL_AMT) AS MIN_OPENING_BAL_AMTFROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_TRAN GROUP BY ACCOUNT_IDHAVING MAX(OPENING_BAL_AMT) > AVG(CLOSING_BAL_AMT)AND MIN_OPENING_BAL_AMT < AVG_CLOSING_BAL_AMT) DT_1ON DT.ACCOUNT_ID =DT_1.ACCOUNT_IDINNER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_GL_BALANCE GLON DT_1.ACCOUNT_ID = GL.ACCOUNT_ID GROUP BY EXECUTION_DT"
cursor = connection.cursor()
cursor.execute(query1)
for row in cursor:
    print(row)

df2 = "DELETE TD_BIM_FR_TRNG_DB.IDEA_T_SEM_ACT_PRD_PRTY_DENORM"
cursor = connection.cursor()
cursor.execute(query2)
connection.commit()

df3 = "INSERT INTO TD_BIM_FR_TRNG_DB.IDEA_T_SEM_ACT_PRD_PRTY_DENORM(ACCOUNT_ID ,SRC_ACCOUNT_NO ,ACCOUNT_SOURCE_CD ,ACCOUNT_CURRENT_STATUS_TYPE_CD,DISPOSITION_CD ,ACCOUNT_BAL_AMT ,ACCOUNT_OPEN_DTTM ,ACCOUNT_CLOSE_DTTM ,ACCOUNT_NAME ,ACCOUNT_DESC_TXT ,PRODUCT_ID ,ACCT_PROD_START_DTTM ,ACCT_PROD_END_DTTM ,PARENT_PRODUCT_ID ,PRODUCT_TYPE_CD ,PRODUCT_DESC ,PRODUCT_NAME ,HOST_PROD_ID ,PRODUCT_START_DT ,PRODUCT_END_DT ,FIN_PRODUCT_ID ,PRODCT_TEXT ,PARTY_ID ,ACCOUNT_TYPE_CD ,ACCOUNT_PARTY_START_DT ,ACCOUNT_PARTY_END_DT ,PARTY_TYPE_CD ,CUST_NO ,CUST_BIRTH_DT ,CUST_GNDR ,PARTY_START_DT ,PARTY_END_DT ,LOAN_TERM_TYPE_CD ,LOAN_ID ,LOAN_ISSUE_DT ,LOAN_AMT ,LOAN_DURATION ,LOAN_STATUS ,LOAN_PAYMENT_COMPLETED ,INTRST_PMT_FREQ_CD ,LOAN_CLOSE_DT ,ACCT_TRANS_REF_NO ,ACCT_TRANS_DT ,OPENING_BAL_AMT ,CLOSING_BAL_AMT ,TRANS_TYPE_CD ,TRANS_CATEG_CD ,TO_BANK_CD ,TO_ACCOUNT_NO ,TOTAL_AMT ,REMAINING_AMT ,MONTHLY_EMI_AMT ,ACCOUNT_GL_BAL_AMT )SELECT ACC.ACCOUNT_ID ,ACC.SRC_ACCOUNT_NO ,CONCAT('DATA SOURCED FROM : ' , ACC.ACCOUNT_SOURCE_CD) AS ACCOUNT_SOURCE_CD ,ACC.ACCOUNT_CURRENT_STATUS_TYPE_CD,ACC.DISPOSITION_CD ,ACC.ACCOUNT_BAL_AMT ,ACC.ACCOUNT_OPEN_DTTM ,ACC.ACCOUNT_CLOSE_DTTM ,ACC.ACCOUNT_NAME ,ACC.ACCOUNT_DESC_TXT ,ACP.PRODUCT_ID ,ACP.ACCT_PROD_START_DTTM ,ACP.ACCT_PROD_END_DTTM ,PR.PARENT_PRODUCT_ID ,PR.PRODUCT_TYPE_CD ,PR.PRODUCT_DESC ,PR.PRODUCT_NAME ,PR.HOST_PROD_ID ,PR.PRODUCT_START_DT ,PR.PRODUCT_END_DT ,PR.FIN_PRODUCT_ID ,PR.PRODCT_TEXT ,AP.PARTY_ID ,AP.ACCOUNT_TYPE_CD ,AP.ACCOUNT_PARTY_START_DT ,AP.ACCOUNT_PARTY_END_DT ,P.PARTY_TYPE_CD ,P.CUST_NO ,P.CUST_BIRTH_DT ,P.CUST_GNDR ,P.PARTY_START_DT ,P.PARTY_END_DT ,LA.LOAN_TERM_TYPE_CD ,LA.LOAN_ID ,LA.LOAN_ISSUE_DT ,LA.LOAN_AMT ,LA.LOAN_DURATION ,LA.LOAN_STATUS ,LA.LOAN_PAYMENT_COMPLETED ,LA.INTRST_PMT_FREQ_CD ,LA.LOAN_CLOSE_DT ,ATR.ACCT_TRANS_REF_NO ,ATR.ACCT_TRANS_DT ,ATR.OPENING_BAL_AMT ,ATR.CLOSING_BAL_AMT ,ATR.TRANS_TYPE_CD ,ATR.TRANS_CATEG_CD ,ATR.TO_BANK_CD ,ATR.TO_ACCOUNT_NO ,LA.LOAN_AMT + CASE WHEN TRANS_TYPE_CD ='D' THEN CASE WHEN TRANS_TYPE_CD ='D' THEN ,OPENING_BAL_AMT ,LOAN_AMT/12 AS MONTHLY_EMI_AMT,ACCOUNT_GL_BAL_AMTFROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT ACC JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PRODUCT ACPON ACC.ACCOUNT_ID = ACP.ACCOUNT_IDAND CAST(ACP.ACCT_PROD_START_DTTM AS DATE ) BETWEEN ACC.ACCOUNT_OPEN_DTTM AND ACC.ACCOUNT_CLOSE_DTTM INNER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PRODUCT PRON ACP.PRODUCT_ID = PR.PRODUCT_IDAND CAST(ACP.ACCT_PROD_START_DTTM AS DATE ) BETWEEN PR.PRODUCT_START_DT AND PR.PRODUCT_END_DT INNER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PARTY APON ACC.ACCOUNT_ID = AP.ACCOUNT_IDAND AP.ACCOUNT_PARTY_END_DT = '9999INNER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY PON AP.PARTY_ID = P.PARTY_IDAND P.PARTY_END_DT > CONVERT(DATE, GETDATE())LEFT JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_LOAN_TERM_ACCT LAON ACC.ACCOUNT_ID = LA.ACCOUNT_IDLEFT JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_TRAN ATRON LA.ACCOUNT_ID = ATR.ACCOUNT_IDINNER JOIN ( SELECT ACCOUNT_ID , ACCOUNT_GL_BAL_DT , ACCOUNT_GL_PST_TYPE_CD, SUM(ACCOUNT_GL_BALANCE_AMT) AS ACCOUNT_GL_BAL_AMTFROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_GL_BALANCE GROUP BY ACCOUNT_ID , ACCOUNT_GL_BAL_DT , ACCOUNT_GL_PST_TYPE_CD ) GLON ATR.ACCOUNT_ID = GL.ACCOUNT_IDAND ATR.ACCT_TRANS_DT = GL.ACCOUNT_GL_BAL_DTLEFT OUTER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PRODUCT ACP_1ON ACC.ACCOUNT_ID = ACP_1.ACCOUNT_IDAND CAST(ACP_1.ACCT_PROD_END_DTTM AS DATE) = '9999WHERE EXISTS (SELECT P_1.PARTY_ID FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY P_1 LEFT JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PARTY AP_1 ON P_1.PARTY_ID = AP_1.PARTY_ID WHERE P.PARTY_ID = P_1.PARTY_ID AND P_1.PARTY_TYPE_CD LIKE 'INDIVIDUAL%' AND AP_1.ACCOUNT_PARTY_END_DT = '9999 GROUP BY P_1.PARTY_ID ) "
cursor = connection.cursor()
cursor.execute(query3)
connection.commit()

