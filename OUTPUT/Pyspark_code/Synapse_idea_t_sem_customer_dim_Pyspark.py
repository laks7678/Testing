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
#Get the github token from environment variables to access github repositoryg = Github(os.environ.get('GITHUB_TOKEN'))
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

spark=SparkSession.builder.appName(cp.get('PySparkProp', 'appName')).getOrCreate()
sc=spark.sparkContext
database =cp.get('SQLSERVERDBConnection', 'database')
user = cp.get('SQLSERVERDBConnection', 'user')
password = cp.get('SQLSERVERDBConnection', 'password')
driver = cp.get('SQLSERVERDBConnection', 'driver')
server = cp.get('SQLSERVERDBConnection', 'server')
connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password}')


df0 = "DELETE CST FROM TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM CST, TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY PARWHERE CST.PARTY_ID = PAR.PARTY_IDAND PAR.PARTY_TYPE_CD<> 'INDIVIDUAL' "
cursor = connection.cursor()
cursor.execute(query0)
connection.commit()

df1 = "UPDATE CST SET CUST_NO = P.CUST_NO ,CUST_BIRTH_DT = P.CUST_BIRTH_DT,CUST_GNDR = P.CUST_GNDR ,PARTY_START_DT =P.PARTY_START_DT ,PARTY_END_DT = P.PARTY_END_DT ,PARTY_SALUTATION =P.PARTY_SALUTATION ,PARTY_FIRST_NM =P.PARTY_FIRST_NM ,PARTY_MIDDLE_NM = P.PARTY_MIDDLE_NM,PARTY_LAST_NM= P.PARTY_LAST_NM FROM TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM CST, TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY P,TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY_LOCATOR PLWHERE CST.PARTY_ID = P.PARTY_IDAND CST.PARTY_TYPE_CD = P.PARTY_TYPE_CDAND P.PARTY_ID=PL.PARTY_IDAND P.PARTY_END_DT='9999AND PL.PARTY_LOC_END_DT='9999UPDATE TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM SET SSN=P.SSN ,PRIMARY_CONTACT_NO=P.PRIMARY_CONTACT_NO ,SECONDARY_CONTACT_NO =P.SECONDARY_CONTACT_NO ,PASSPORT_NO =P.PASSPORT_NO ,DEA_NO=P.DEA_NO ,TAX_ID =P.TAX_ID FROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY P,TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY_LOCATOR PLWHERE TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.PARTY_ID = P.PARTY_IDAND TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.PARTY_TYPE_CD = P.PARTY_TYPE_CDAND P.PARTY_ID=PL.PARTY_IDAND P.PARTY_END_DT='9999AND PL.PARTY_LOC_END_DT='9999UPDATE TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM SET DISTRICT_ID = PL.DISTRICT_ID,DISTRICT_NAME = PL.DISTRICT_NAME ,PARTY_RGN = PL.PARTY_RGN ,PARTY_LOC_START_DT= PL.PARTY_LOC_START_DT ,PARTY_LOC_END_DT = PL.PARTY_LOC_END_DT ,HOUSE_NO= PL.HOUSE_NO ,STREET_NO = PL.STREET_NO ,STREET_NAME = PL.STREET_NAME ,ADDRESS_LINE_1_TXT= PL.ADDRESS_LINE_1_TXT ,ADDRESS_LINE_2_TXT= PL.ADDRESS_LINE_2_TXT ,CITY_NM = PL.CITY_NM ,STATE_NM = PL.STATE_NM ,COUNTRY_NM = PL.COUNTRY_NM ,POSTAL_CD = PL.POSTAL_CDFROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY P,TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY_LOCATOR PLWHERE TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.PARTY_ID = P.PARTY_IDAND TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.PARTY_TYPE_CD = P.PARTY_TYPE_CDAND P.PARTY_ID=PL.PARTY_IDAND P.PARTY_END_DT='9999AND PL.PARTY_LOC_END_DT='9999AND ( COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.DISTRICT_ID,123) <> COALESCE(PL.DISTRICT_ID,123) OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.DISTRICT_NAME,'') <> COALESCE(PL.DISTRICT_NAME,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.PARTY_RGN,'') <> COALESCE(PL.PARTY_RGN,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.HOUSE_NO,'') <> COALESCE(PL.HOUSE_NO,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.STREET_NO,'') <> COALESCE(PL.STREET_NO,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.STREET_NAME,'') <> COALESCE(PL.STREET_NAME,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.ADDRESS_LINE_1_TXT,'') <> COALESCE(PL.ADDRESS_LINE_1_TXT,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.ADDRESS_LINE_2_TXT,'') <> COALESCE(PL.ADDRESS_LINE_2_TXT,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.CITY_NM,'') <> COALESCE(PL.CITY_NM,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.STATE_NM,'') <> COALESCE(PL.STATE_NM,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.COUNTRY_NM,'') <> COALESCE(PL.COUNTRY_NM,'')OR COALESCE(TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM.POSTAL_CD,'') <> COALESCE(PL.POSTAL_CD,''))"
cursor = connection.cursor()
cursor.execute(query1)
connection.commit()

df2 = "UPDATE TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM SET SEM_INS_DT = CONVERT(DATE, GETDATE()) ,SEM_INS_DTTM = CONVERT(DATETIME, GETDATE()) ,SEM_INS_TM = CONVERT(TIME, GETDATE())"
cursor = connection.cursor()
cursor.execute(query2)
connection.commit()

df3 = "INSERT TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM(PARTY_ID ,PARTY_TYPE_CD ,CUST_NO ,CUST_BIRTH_DT ,CUST_GNDR ,PARTY_START_DT ,PARTY_END_DT ,PARTY_SALUTATION ,PARTY_FIRST_NM ,PARTY_MIDDLE_NM ,PARTY_LAST_NM ,SSN ,PRIMARY_CONTACT_NO ,SECONDARY_CONTACT_NO ,PASSPORT_NO ,DEA_NO ,TAX_ID ,DISTRICT_ID ,DISTRICT_NAME ,PARTY_RGN ,PARTY_LOC_START_DT ,PARTY_LOC_END_DT ,HOUSE_NO ,STREET_NO ,STREET_NAME ,ADDRESS_LINE_1_TXT ,ADDRESS_LINE_2_TXT ,CITY_NM ,STATE_NM ,COUNTRY_NM ,POSTAL_CD ,SEM_INS_DT ,SEM_INS_DTTM ,SEM_INS_TM ,SEM_UPD_DT,SEM_UPD_DTTM,SEM_UPD_TM)SELECT P.PARTY_ID,P.PARTY_TYPE_CD,P.CUST_NO,P.CUST_BIRTH_DT,P.CUST_GNDR,P.PARTY_START_DT,P.PARTY_END_DT,P.PARTY_SALUTATION,P.PARTY_FIRST_NM,RIGHT(P.PARTY_MIDDLE_NM,15) PARTY_MIDDLE_NM,LEFT(P.PARTY_LAST_NM,15) PARTY_LAST_NM,CASE WHEN LEN(P.SSN) <= 20 THEN P.SSN ELSE '000'+P.SSN END AS SSN ,CASE WHEN P.PRIMARY_CONTACT_NO<> '0000' THEN P.PRIMARY_CONTACT_NO ELSE 'NO PRIMARY CONTACT NO' END PRIMARY_CONTACT_NO,CASE WHEN LEN(P.SECONDARY_CONTACT_NO) >= 10 THEN P.SECONDARY_CONTACT_NO ELSE '000'+P.SECONDARY_CONTACT_NO END AS SECONDARY_CONTACT_NO ,CASE WHEN LEN(P.PASSPORT_NO) > 6 THEN P.PASSPORT_NO ELSE '000'+P.PASSPORT_NO END AS PASSPORT_NO ,CASE WHEN LEN(P.DEA_NO) = 11 THEN P.DEA_NO ELSE '000'+P.DEA_NO END AS DEA_NO,CASE WHEN LEN(P.TAX_ID) < 10 THEN '000'+P.TAX_ID ELSE P.TAX_ID END AS TAX_ID ,PL.DISTRICT_ID,PL.DISTRICT_NAME,PL.PARTY_RGN,PL.PARTY_LOC_START_DT,PL.PARTY_LOC_END_DT,PL.HOUSE_NO,PL.STREET_NO,PL.STREET_NAME,PL.ADDRESS_LINE_1_TXT,PL.ADDRESS_LINE_2_TXT,PL.CITY_NM,PL.STATE_NM,PL.COUNTRY_NM,PL.POSTAL_CD,CONVERT(DATE, GETDATE()) AS SEM_INS_DT,CONVERT(DATETIME, GETDATE()) AS SEM_INS_DTTM,CONVERT(TIME, GETDATE()) AS SEM_INS_TM,CONVERT(DATE, GETDATE()) AS SEM_UPD_DT,CONVERT(DATETIME, GETDATE()) AS SEM_UPD_DTTM,CONVERT(TIME, GETDATE()) AS SEM_UPD_TMFROM TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY PINNER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_TGT_PARTY_LOCATOR PLON P.PARTY_ID=PL.PARTY_IDLEFT OUTER JOIN TD_BIM_FR_TRNG_DB.IDEA_T_SEM_CUSTOMER_DIM SCDON P.PARTY_ID =SCD.PARTY_IDAND P.PARTY_TYPE_CD = SCD.PARTY_TYPE_CDWHERE P.PARTY_END_DT='9999AND PL.PARTY_LOC_END_DT='9999AND SCD.PARTY_ID IS NULL"
cursor = connection.cursor()
cursor.execute(query3)
connection.commit()

