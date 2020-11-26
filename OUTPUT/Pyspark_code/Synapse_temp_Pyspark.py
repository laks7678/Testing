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


query0 = "CREATE TABLE TD_BIM_FR_TRNG_DB.IDEA_T_SEM_ACCOUNT_PRODUCT_REL ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO ( ACCOUNT_ID BIGINT, SRC_ACCOUNT_NO DECIMAL(18,4), ACCOUNT_TYPE_CD VARCHAR(15) , ACCOUNT_SOURCE_CD VARCHAR(10) , ACCOUNT_CURRENT_STATUS_TYPE_CD CHAR(20) , DISPOSITION_TXT VARCHAR(15) , ACCOUNT_BAL_AMT DECIMAL(38,8), ACCOUNT_OPEN_DTTM DATE, ACCOUNT_CLOSE_DTTM DATETIME2(4), ACCOUNT_NAME NVARCHAR(1000) , PRODUCT_ID INT, PARENT_PRODUCT_ID BIGINT, PRODUCT_TYPE_CD CHAR(15) , PRODUCT_DESC NVARCHAR(255) , PRODUCT_NAME VARCHAR(255) , HOST_PROD_ID VARCHAR(50) , PRODUCT_START_DT DATE, PRODUCT_END_DT DATE, FIN_PRODUCT_ID INT, ACCOUNT_PROD_DESC NVARCHAR(4000) )"
cursor = connection.cursor()
cursor.execute(query0)
connection.commit()

query1 = " CREATE TABLE #EMPLOYEE ( EMPLOYEENO INT, FIRSTNAME VARCHAR(30) , LASTNAME NVARCHAR(30) , DOB DATE, JOINEDDATE DATE, DEPARTMENTNO SMALLINT)"
cursor = connection.cursor()
cursor.execute(query1)
connection.commit()

