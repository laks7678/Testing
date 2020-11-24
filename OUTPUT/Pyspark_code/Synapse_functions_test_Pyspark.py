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


query0 = "SELECT CONVERT(DATE, GETDATE()) ,CONVERT(TIME, GETDATE()) ,CONVERT(TIME, GETDATE()),CONVERT(DATETIME, GETDATE()),('2020,EOMONTH('2016,ROUND('2020,ROUND('2020,ROUND('2020,ROUND('2020,('2020,('2020,datepart( YY, '2020,datepart( MM, '2020,datepart( DD, '2020,datepart( HH, '2020,datepart( MI, '2020,datepart( SS, '2020,'2020,'2020,'2020,'2020,'2020,'2020,'2020,CAST((9999* INTERVAL '1' SECOND) AS TO MINUTE),CAST((36600* INTERVAL '00:00:01' HOUR TO SECOND) AS TO MINUTE),COALESCE(NULL,NULL,'TERADATA') ,('TERADATA'),(454.44),(CONVERT(TIME, GETDATE())),(CONVERT(DATE, GETDATE())),CASE WHEN 1=1 THEN'SURENDRA' ELSE 'REDDY' END ,SQRT(81),POWER(9,2),26 % 5,ABS(500),SIGN(,EXP(3),(7.387524),LOG(100),RAND(1,100),ROUND(35.222,1),FLOOR(55.63),CEILING(55.63),NULLIF(0,0),ISNULL(NULL,0),(12,434,21,543,243,111),(12,434,21,543,243,111),LOWER('TERADATA'),'IDEA'+'PROJECT',UPPER('TERADATA'),('TERADATA LEARNING'),LEN('TERADATA'),LEN('TERADATA'),LEN('TERADATA'),LEN('TERADATA'),CHARINDEX('D','TERADATA'),CHARINDEX('D' , 'TERADATA'),ASCII('A'),CHAR(65),TRANSLATE('TERADATA' USING UNICODE_TO_LATIN),SUBSTRING(TERA_DATA,3,len(TERA_DATA),LEFT('PRADEEP',3),RIGHT('PRADEEP',3),REVERSE('SURENDRA'),TRIM(' TERADATA '),reverse(Substring(reverse('SSSTERADATASSS'),Patindex('%[^reverse()]%',reverse('SSSTERADATASSS')+''),Len(reverse('SSSTERADATASSS')))),LTRIM(' TERADATA '),RTRIM(' TERADATA '),REPLACE('TERADATA SUPPORT HIGH VOLUME OF DATA','DATA','BINARY'),('SPEAK LESS','PKA','LPE'),LEFT('TERADATA',10),RIGHT('TERADATA',10),RIGHT('TERADATA',5,'<3U'),DATEPART ( dw , CONVERT(DATE, GETDATE())),DATEPART ( mm , CONVERT(DATE, GETDATE())),DATEPART ( yy , CONVERT(DATE, GETDATE())),(CONVERT(DATE, GETDATE())),(CONVERT(DATE, GETDATE())),DATEPART ( wk , CONVERT(DATE, GETDATE())),(CONVERT(DATE, GETDATE())),(CONVERT(DATE, GETDATE())),DATEPART ( mm , CONVERT(DATE, GETDATE())),(CONVERT(DATE, GETDATE())),DATEPART ( q ,CONVERT(DATE, GETDATE())),DATEPART ( yy ,CONVERT(DATE, GETDATE())),MAX(1),MIN(1),SUM(1),COUNT(1),('TRACTOR,SCOOTER,BIKE,','[S][A, ('TRACTOR,SCOOTER,BIKE','[S][A, ('TRACTOR,SCOOTER,BIKE,SUMMER','[S][AFROM TD_BIM_FR_TRNG_DB . IDEA_T_STG_FIN_LOAN"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

query1 = "CREATE TABLE #ACCOUNT"
cursor = connection.cursor()
cursor.execute(query1)
connection.commit()

query2 = "SELECT * FROM ACCOUNT"
cursor = connection.cursor()
cursor.execute(query2)
for row in cursor:
    print(row)

query3 = "SELECT ACC FROM ACCOUNT"
cursor = connection.cursor()
cursor.execute(query3)
for row in cursor:
    print(row)

query4 = "DELETE FROM ACCOUNT"
cursor = connection.cursor()
cursor.execute(query4)
connection.commit()

query5 = "DELETE FROM ACCOUNT"
cursor = connection.cursor()
cursor.execute(query5)
connection.commit()

