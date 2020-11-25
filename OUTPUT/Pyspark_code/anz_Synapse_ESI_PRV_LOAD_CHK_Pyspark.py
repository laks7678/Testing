import sys
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

import logging
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

import configparser
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

from github import Github
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

import findspark
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

findspark.init()
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

findspark.find()
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

import pyspark
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

from pyspark import SparkContext, SparkConf, SQLContext
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

from pyspark.sql import SparkSession
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

import pyodbc
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

import pandas as pd
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

from pyspark.sql import functions as F
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

from pyspark.sql.functions import lit, col
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

from _io import StringIO
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)


cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

spark=SparkSession.builder.appName("Building Pyspark code with Synapse statements embedded").getOrCreate()
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

sc=spark.sparkContext
cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)


cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)


cp = configparser.ConfigParser()
g = Github('99ce326ab3f30606e3ff0a81476e6b0d04835cfc')
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


query0 = "SELECT LOAD_LOG_KEY FROM $TGT_LOAD_LOGWHERE WORK_FLOW_NM='$WORK_FLOW_NM'AND PBLSH_IND = 'N'AND LOAD_END_DTM = ( SELECT MAX (LOAD_END_DTM) FROM $TGT_LOAD_LOG WHERE SUBJ_AREA_NM = TRIM('$SUBJ_AREA_NM') )"
cursor = connection.cursor()
cursor.execute(query0)
for row in cursor:
    print(row)

