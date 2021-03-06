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
#Get the github token from environment variables to access github repository
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
            #Reading property file
            cp.read_file(StringIO(file_content.decoded_content.decode()))

#Creating spark session
spark=SparkSession.builder.appName(cp.get('PySparkProp', 'appName')).getOrCreate()
sc=spark.sparkContext
database =cp.get('SQLSERVERDBConnection', 'database')
user = cp.get('SQLSERVERDBConnection', 'user')
password = cp.get('SQLSERVERDBConnection', 'password')
driver = cp.get('SQLSERVERDBConnection', 'driver')
server = cp.get('SQLSERVERDBConnection', 'server')
connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password}')


#Creating dataframes for parsing the tables
df0 = "DELETE TD_BIM_FR_TRNG_DB.IDEA_T_SEM_GL_BALANCE_FACT"
cursor = connection.cursor()
cursor.execute(df0)
connection.commit()

#Creating dataframes for parsing the tables
df1 = "INSERT INTO TD_BIM_FR_TRNG_DB.IDEA_T_SEM_GL_BALANCE_FACTSELECT GL_TRN.ACCOUNT_ID, GL_TRN.ACCOUNT_GL_BAL_DT ,SUM(GL.ACCOUNT_GL_BALANCE_AMT) AS TOTAL_ACCOUNT_GL_BAL_AMT,MAX(GL.ACCOUNT_GL_BALANCE_AMT) AS MAX_ACCOUNT_GL_BAL_AMT,MIN(GL.ACCOUNT_GL_BALANCE_AMT) AS MIN_ACCOUNT_GL_BAL_AMT,AVG(GL.ACCOUNT_GL_BALANCE_AMT) AS AVG_ACCOUNT_GL_BAL_AMT,TRN.AVERAGE_OPENING_BAL_AMT,TRN.MAX_OPENING_BAL_AMT,TRN.TOT_CLOSING_BAL_AMT,TRN.MIN_CLOSING_BAL_AMT,PL.PARTY_RGN ,PL.PARTY_LOC_START_DT ,PL.PARTY_LOC_END_DT ,PL.HOUSE_NO ,PL.STREET_NO ,PL.STREET_NAME ,PL.ADDRESS_LINE_1_TXT ,PL.ADDRESS_LINE_2_TXT ,PL.CITY_NM ,PL.STATE_NM ,PL.COUNTRY_NM,PL.POSTAL_CDFROM (SELECT ACCOUNT_ID, ACCOUNT_GL_BAL_DT FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_GL_BALANCEUNION SELECT ACCOUNT_ID, ACCT_TRANS_DT FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_TRANEXCEPTSELECT ACCOUNT_ID, ACCOUNT_GL_BAL_DT FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_GL_BALANCE WHERE ACCOUNT_GL_PST_TYPE_CD='CIC' INTERSECT SELECT ACCOUNT_ID, ACCOUNT_GL_BAL_DT FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_GL_BALANCE WHERE ACCOUNT_GL_PST_TYPE_CD IN ('ROB','WIC')UNION ALLSELECT ACCOUNT_ID, ACCOUNT_GL_BAL_DT FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_GL_BALANCE WHERE ACCOUNT_GL_PST_TYPE_CD IN ('ROB','WIC')) GL_TRNINNER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_GL_BALANCE GLON GL_TRN.ACCOUNT_ID = GL.ACCOUNT_IDAND GL_TRN.ACCOUNT_GL_BAL_DT = GL.ACCOUNT_GL_BAL_DTLEFT JOIN (SELECT ACCOUNT_ID,ACCT_TRANS_REF_NO,ACCT_TRANS_DT,AVG(OPENING_BAL_AMT) AS AVERAGE_OPENING_BAL_AMT,MAX(OPENING_BAL_AMT) AS MAX_OPENING_BAL_AMT, SUM(CLOSING_BAL_AMT) AS TOT_CLOSING_BAL_AMT, MIN(CLOSING_BAL_AMT) AS MIN_CLOSING_BAL_AMT,TRANS_TYPE_CD,TRANS_CATEG_CD,TO_BANK_CD,TO_ACCOUNT_NOFROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_TRAN WHERE TRANS_CATEG_CD <> '' AND TO_BANK_CD IS NOT NULLGROUP BY ACCOUNT_ID,ACCT_TRANS_REF_NO,ACCT_TRANS_DT,TRANS_TYPE_CD,TRANS_CATEG_CD,TO_BANK_CD,TO_ACCOUNT_NO) TRNON GL.ACCOUNT_ID = TRN.ACCOUNT_IDAND GL.ACCOUNT_GL_BAL_DT = TRN.ACCT_TRANS_DTINNER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_ACCOUNT_PARTY APON GL.ACCOUNT_ID = AP.ACCOUNT_IDAND AP.ACCOUNT_PARTY_END_DT >= CONVERT(DATE, GETDATE())LEFT JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY_LOCATOR PLON AP.PARTY_ID = PL.PARTY_IDAND PL.PARTY_LOC_END_DT = '9999GROUP BY GL_TRN.ACCOUNT_ID,GL_TRN.ACCOUNT_GL_BAL_DT,TRN.AVERAGE_OPENING_BAL_AMT,TRN.MAX_OPENING_BAL_AMT,TRN.TOT_CLOSING_BAL_AMT,TRN.MIN_CLOSING_BAL_AMT,PL.PARTY_RGN ,PL.PARTY_LOC_START_DT ,PL.PARTY_LOC_END_DT ,PL.HOUSE_NO ,PL.STREET_NO ,PL.STREET_NAME ,PL.ADDRESS_LINE_1_TXT ,PL.ADDRESS_LINE_2_TXT ,PL.CITY_NM ,PL.STATE_NM ,PL.COUNTRY_NM,PL.POSTAL_CD"
cursor = connection.cursor()
cursor.execute(df1)
connection.commit()

