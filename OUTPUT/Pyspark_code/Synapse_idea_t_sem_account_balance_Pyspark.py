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
g = Github('25b4b6bc2587173d3791ea3e8be0a6304dcdd53')
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


query0 = "DELETE APR FROM TD_BIM_FR_TRNG_DB.IDEA_T_SEM_ACCOUNT_BALANCE APR WHERE EXISTS (SELECT 1 FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT ACT WHERE APR.ACCOUNT_ID = ACT.ACCOUNT_ID )"
cursor = connection.cursor()
cursor.execute(query0)
connection.commit()

query1 = "INSERT INTO TD_BIM_FR_TRNG_DB.IDEA_T_SEM_ACCOUNT_BALANCESELECT ACT.ACCOUNT_ID, ACT_PRD.PRODUCT_ID, ROW_NUMBER() OVER(ORDER BY ACT.ACCOUNT_ID) AS ACCT_SEQ_NO, RANK() OVER(ORDER BY ACT_PRD.PRODUCT_ID) AS PROD_SEQ_NO, SUM(CASE WHEN LON.LOAN_AMT IS NOT NULL THEN LON.LOAN_AMT ELSE ACT.ACCOUNT_BAL_AMT END ) OVER(PARTITION BY ACT.ACCOUNT_ID ORDER BY ACT.ACCOUNT_OPEN_DTTM) AS CUM_LOAN_BAL_AMT, SUM(ACT.ACCOUNT_BAL_AMT) OVER (PARTITION BY ACT_PRD.PRODUCT_ID ) AS BAL_AMT_PROD, SUM(CASE WHEN LON.LOAN_AMT IS NOT NULL THEN LON.LOAN_AMT ELSE ACT.ACCOUNT_BAL_AMT END ) OVER (PARTITION BY ACT_PRD.PRODUCT_ID ORDER BY ACT.ACCOUNT_OPEN_DTTM ROWS UNBOUNDED PRECEDING) AS CUM_BAL_AMT_PER_PROD, SUM(CASE WHEN LON.LOAN_AMT IS NOT NULL THEN LON.LOAN_AMT ELSE ACT.ACCOUNT_BAL_AMT END ) OVER (PARTITION BY ACT_PRD.ACCOUNT_ID ORDER BY ACT.ACCOUNT_OPEN_DTTM ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS CUM_BAL_AMT_PROD_BLOCK, ROW_NUMBER() OVER (PARTITION BY ACT_PRD.PRODUCT_ID ORDER BY ACT.ACCOUNT_ID) AS PROD_LVL_SEQ_NO, RANK() OVER (PARTITION BY ACT_PRD.PRODUCT_ID ORDER BY ACT.ACCOUNT_ID) AS BLOCK_LVL_SEQ_NO, DENSE_RANK() OVER (PARTITION BY ACT_PRD.PRODUCT_ID ORDER BY ACT.ACCOUNT_ID) AS BLOCK_LVL_INCR_NO, MAX(LOAN_AMT) OVER (PARTITION BY LON.ACCOUNT_ID) AS MAX_LOAN_AMT, MIN(ACT.ACCOUNT_BAL_AMT) OVER(PARTITION BY ACT.ACCOUNT_ID ) MIN_BAL_AMT, AVG(LOAN_AMT) OVER (PARTITION BY LON.ACCOUNT_ID) AS AVG_LOAN_AMT FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PRODUCT ACT_PRDRIGHT OUTER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT ACTON ACT.ACCOUNT_ID = ACT_PRD.ACCOUNT_IDLEFT JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_LOAN_TERM_ACCT LONON ACT.ACCOUNT_ID = LON.ACCOUNT_IDWHERE ACT.ACCOUNT_TYPE_CD ='SAVINGS' "
cursor = connection.cursor()
cursor.execute(query1)
connection.commit()

