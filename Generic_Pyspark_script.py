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
