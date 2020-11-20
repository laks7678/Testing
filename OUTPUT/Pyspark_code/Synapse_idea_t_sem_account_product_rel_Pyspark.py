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
g = Github("6707170a792c9abc8b0f69fe5151daa7d0644a95")
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


query0 = "DELETE FROM TD_BIM_FR_TRNG_DB.IDEA_T_SEM_ACCOUNT_PRODUCT_REL "
cursor = connection.cursor()
cursor.execute(query0)
connection.commit()

query1 = "INSERT INTO TD_BIM_FR_TRNG_DB.IDEA_T_SEM_ACCOUNT_PRODUCT_REL ( ACCOUNT_ID, SRC_ACCOUNT_NO, ACCOUNT_TYPE_CD, ACCOUNT_SOURCE_CD, ACCOUNT_CURRENT_STATUS_TYPE_CD, DISPOSITION_TXT, ACCOUNT_BAL_AMT, ACCOUNT_OPEN_DTTM, ACCOUNT_CLOSE_DTTM, ACCOUNT_NAME, PRODUCT_ID, PARENT_PRODUCT_ID, PRODUCT_TYPE_CD, PRODUCT_DESC, PRODUCT_NAME, HOST_PROD_ID, PRODUCT_START_DT, PRODUCT_END_DT, FIN_PRODUCT_ID, ACCOUNT_PROD_DESC ) SELECT DISTINCT SUM(ACCOUNT_ID) OVER (ORDER BY SRC_ACCOUNT_NO ROWS UNBOUNDED PRECEDING) , SUM(1) OVER (ORDER BY SRC_ACCOUNT_NO ROWS 6-1 PRECEDING) , SUM(1) OVER (ORDER BY SRC_ACCOUNT_NO ASC ROWS 6-1 PRECEDING) , SUM(1) OVER (ORDER BY SRC_ACCOUNT_NO DESC ROWS 6-1 PRECEDING) , ACT.ACCOUNT_ID, ACT.SRC_ACCOUNT_NO, LTRIM(ACT.ACCOUNT_TYPE_CD) ACCOUNT_TYPE_CD, RTRIM(ACT.ACCOUNT_SOURCE_CD), ACT.ACCOUNT_CURRENT_STATUS_TYPE_CD, CASE WHEN ACT.DISPOSITION_CD LIKE 'O%' THEN ACT.DISPOSITION_CD+'-'+'AS PER SOURCE' ELSE 'UNKNOWN VALUE'END AS DISPOSITION_TXT, ACT.ACCOUNT_BAL_AMT, CAST(ACT.ACCOUNT_OPEN_DTTM AS DATE), ACT.ACCOUNT_CLOSE_DTTM, CAST(ACT.ACCOUNT_NAME AS VARCHAR(1000)), PRD.PRODUCT_ID ,PRD.PARENT_PRODUCT_ID ,PRD.PRODUCT_TYPE_CD ,PRD.PRODUCT_DESC ,PRD.PRODUCT_NAME ,PRD.HOST_PROD_ID ,PRD.PRODUCT_START_DT ,PRD.PRODUCT_END_DT ,PRD.FIN_PRODUCT_ID ,COALESCE( ACT.ACCOUNT_DESC_TXT,'UNKNOWN ACCOUNT') +'--'+ COALESCE(TRIM(PRD.PRODCT_TEXT),'UNKNOWN PRODUCT') AS ACCOUNT_PROD_DESC FROM (SELECT DISTINCT * FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT ) ACT JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PRODUCT ACT_PRD ON ACT.ACCOUNT_ID = ACT_PRD.ACCOUNT_ID LEFT JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PRODUCT PRD ON ACT_PRD.PRODUCT_ID = PRD.PRODUCT_ID AND PRD.PRODUCT_END_DT = '9999-12-31' WHERE CAST(ACT.ACCOUNT_CLOSE_DTTM AS DATE ) = '9999-12-31'"
cursor = connection.cursor()
cursor.execute(query1)
connection.commit()

