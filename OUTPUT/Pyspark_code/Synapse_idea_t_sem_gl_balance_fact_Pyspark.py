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


query0 = "DELETE dbo.IDEA_T_SEM_GL_BALANCE_FACT"
cursor = connection.cursor()
cursor.execute(query0)
connection.commit()

query1 = "INSERT INTO dbo.IDEA_T_SEM_GL_BALANCE_FACT SELECT GL_TRN.ACCOUNT_ID, GL_TRN.ACCOUNT_GL_BAL_DT , SUM(GL.ACCOUNT_GL_BALANCE_AMT) AS TOTAL_ACCOUNT_GL_BAL_AMT, MAX(GL.ACCOUNT_GL_BALANCE_AMT) AS MAX_ACCOUNT_GL_BAL_AMT, MIN(GL.ACCOUNT_GL_BALANCE_AMT) AS MIN_ACCOUNT_GL_BAL_AMT, AVG(GL.ACCOUNT_GL_BALANCE_AMT) AS AVG_ACCOUNT_GL_BAL_AMT, TRN.AVERAGE_OPENING_BAL_AMT, TRN.MAX_OPENING_BAL_AMT, TRN.TOT_CLOSING_BAL_AMT, TRN.MIN_CLOSING_BAL_AMT, PL.PARTY_RGN ,PL.PARTY_LOC_START_DT ,PL.PARTY_LOC_END_DT ,PL.HOUSE_NO ,PL.STREET_NO ,PL.STREET_NAME ,PL.ADDRESS_LINE_1_TXT ,PL.ADDRESS_LINE_2_TXT ,PL.CITY_NM ,PL.STATE_NM ,PL.COUNTRY_NM ,PL.POSTAL_CD FROM ( SELECT ACCOUNT_ID, ACCOUNT_GL_BAL_DT FROM dbo.IDEA_T_TGT_GL_BALANCE UNION SELECT ACCOUNT_ID, ACCT_TRANS_DT FROM dbo.IDEA_T_TGT_ACCOUNT_TRAN EXCEPT SELECT ACCOUNT_ID, ACCOUNT_GL_BAL_DT FROM dbo.IDEA_T_TGT_GL_BALANCE WHERE ACCOUNT_GL_PST_TYPE_CD='CIC' INTERSECT SELECT ACCOUNT_ID, ACCOUNT_GL_BAL_DT FROM dbo.IDEA_T_TGT_GL_BALANCE WHERE ACCOUNT_GL_PST_TYPE_CD IN ('ROB','WIC') UNION ALL SELECT ACCOUNT_ID, ACCOUNT_GL_BAL_DT FROM dbo.IDEA_T_TGT_GL_BALANCE WHERE ACCOUNT_GL_PST_TYPE_CD IN ('ROB','WIC') ) GL_TRN INNER JOIN dbo.IDEA_T_TGT_GL_BALANCE GL ON GL_TRN.ACCOUNT_ID = GL.ACCOUNT_ID AND GL_TRN.ACCOUNT_GL_BAL_DT = GL.ACCOUNT_GL_BAL_DT LEFT JOIN (SELECT ACCOUNT_ID,ACCT_TRANS_REF_NO,ACCT_TRANS_DT,AVG(OPENING_BAL_AMT) AS AVERAGE_OPENING_BAL_AMT, MAX(OPENING_BAL_AMT) AS MAX_OPENING_BAL_AMT, SUM(CLOSING_BAL_AMT) AS TOT_CLOSING_BAL_AMT, MIN(CLOSING_BAL_AMT) AS MIN_CLOSING_BAL_AMT, TRANS_TYPE_CD,TRANS_CATEG_CD,TO_BANK_CD,TO_ACCOUNT_NO FROM dbo.IDEA_T_TGT_ACCOUNT_TRAN WHERE TRANS_CATEG_CD <> '' AND TO_BANK_CD IS NOT NULL GROUP BY ACCOUNT_ID,ACCT_TRANS_REF_NO,ACCT_TRANS_DT,TRANS_TYPE_CD,TRANS_CATEG_CD,TO_BANK_CD,TO_ACCOUNT_NO ) TRN ON GL.ACCOUNT_ID = TRN.ACCOUNT_ID AND GL.ACCOUNT_GL_BAL_DT = TRN.ACCT_TRANS_DT INNER JOIN dbo.IDEA_T_TGT_ACCOUNT_PARTY AP ON GL.ACCOUNT_ID = AP.ACCOUNT_ID AND AP.ACCOUNT_PARTY_END_DT >= CONVERT(DATE, GETDATE()) LEFT JOIN dbo.IDEA_T_TGT_PARTY_LOCATOR PL ON AP.PARTY_ID = PL.PARTY_ID AND PL.PARTY_LOC_END_DT = '9999-12-31' GROUP BY GL_TRN.ACCOUNT_ID,GL_TRN.ACCOUNT_GL_BAL_DT,TRN.AVERAGE_OPENING_BAL_AMT,TRN.MAX_OPENING_BAL_AMT,TRN.TOT_CLOSING_BAL_AMT, TRN.MIN_CLOSING_BAL_AMT,PL.PARTY_RGN ,PL.PARTY_LOC_START_DT ,PL.PARTY_LOC_END_DT ,PL.HOUSE_NO ,PL.STREET_NO ,PL.STREET_NAME ,PL.ADDRESS_LINE_1_TXT ,PL.ADDRESS_LINE_2_TXT ,PL.CITY_NM ,PL.STATE_NM ,PL.COUNTRY_NM ,PL.POSTAL_CD"
cursor = connection.cursor()
cursor.execute(query1)
connection.commit()
