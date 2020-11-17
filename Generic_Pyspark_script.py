import sys
import logging
import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
import pyodbc
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col
appName = "PySpark SQL Server Example - via ODBC"
master = "local"
conf = SparkConf().setAppName(appName).setMaster(master) 
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

database = "ideasqldb"
user = "dbadmin"
password  = "Password##123"
connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=ideasqlserver.database.windows.net,1433;DATABASE={database};UID={user};PWD={password}')
