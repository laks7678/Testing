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


query4 = "INSERT INTO AUDT_STTSTC( AUDT_RULE_ID, EVNT_DTM, PRD_STRT_DT, PRD_END_DT, FILE_NM, SESN_NM, MAPG_NM, RCRD_CNT, CLMN_TOTL, TBL_TOTL, LOAD_LOG_KEY, LOAD_PRCS_CD, TBL_NM )SELECT AUDT_RULE_ID, CONVERT(DATETIME, GETDATE()), CONVERT(DATE, GETDATE()), CONVERT(DATE, GETDATE()), 'N/A', 'N/A', 'N/A', CNTALL, SUMCLMN, 0, LOAD_LOG_KEY, '$TGT_LOAD_PRCS_CD' AS LOAD_PRCS_CD, '$TBL_NM' AS TBL_NMFROM (SELECT COUNT(*), COALESCE(SUM(CASE WHEN $CLMN_NM=0 THEN 0 ELSE $CLMN_NM END),0) FROM $TBL_NM WHERE LOAD_LOG_KEY= ( SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOG WHERE LOAD_STRT_DTM = ( SELECT MAX (LOAD_STRT_DTM) LOAD_STRT_DTM FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') AND WORK_FLOW_NM = TRIM('$WORK_FLOW_NM') AND PBLSH_IND = 'Y' ) )) AS TEMP2(CNTALL,SUMCLMN), (SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOG WHERE LOAD_STRT_DTM = ( SELECT MAX(LOAD_STRT_DTM) LOAD_STRT_DTM FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') AND WORK_FLOW_NM = TRIM('$WORK_FLOW_NM') AND PBLSH_IND = 'Y' ) )AS TEMP3(LOAD_LOG_KEY), (SELECT AUDT_RULE_ID FROM AUDT_BLNCG_RULE WHERE TBL_NM = TRIM('$TBL_NM') AND CLMN_NM = TRIM('$CLMN') AND ENVRNMNT_CD = '$TGT_LOAD_PRCS_CD' ) AS TEMP4(AUDT_RULE_ID)"
cursor = connection.cursor()
cursor.execute(query4)
connection.commit()

