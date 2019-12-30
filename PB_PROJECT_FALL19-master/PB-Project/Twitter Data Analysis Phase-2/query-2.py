# Python version 2.7.6
# Import the datetime and pytz modules.
import datetime, pytz
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys,tweepy,csv,re
from textblob import TextBlob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches
datetime_obj = datetime.datetime.strptime('Sat Nov 9 7:15:38 +0000 2019', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Sat Nov 9 7:15:38 +0000 2019','%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
df.createOrReplaceTempView("nba")
sqldf= spark.sql("SELECT 'Staples Center' Arena,'Los Angeles' City,count(*) FROM nba WHERE upper(text) LIKE '%LOS ANGELES%' or text like '%los angeles%' \
UNION \
SELECT 'Amway Center' Arena,'Orlando' City,count(*) FROM nba WHERE upper(text) LIKE '%ORLANDO%' or text like '%orlando%' \
UNION \
SELECT 'TD Garden' Arena,'Boston' City,count(*) FROM nba WHERE upper(text) LIKE '%BOSTON' or text like '%boston%' \
UNION \
SELECT 'American Airlines Center' Arena,'Dallas' City,count(*) FROM nba WHERE upper(text) LIKE '%DALLAS%' or text like '%dallas%' \
UNION \
SELECT 'Madison Square Garden' Arena,'New York' City,count(*) FROM nba WHERE upper(text) LIKE '%NEW YORK%' or text like '%new york%' \
UNION \
SELECT 'Veterans Memorial Coliseum' Arena,'Portland' City,count(*) FROM nba WHERE upper(text) LIKE '%PORTLAND%' or text like '%portland%' \
UNION \
SELECT 'Wells Fargo Center' Arena,'Philadelphia' City,count(*) FROM nba WHERE upper(text) LIKE '%PHILADELPHIA%' or text like '%philadelphia%' \
UNION \
SELECT 'Golden 1 Center' Arena,'Sacramento' City,count(*) FROM nba WHERE upper(text) LIKE '%SACRAMENTO%' or text like '%sacramento%' \
UNION \
SELECT 'Barclays Center' Arena,'Brooklyn' City,count(*) FROM nba WHERE upper(text) LIKE '%BROOKLYN%' or text like '%brooklyn%' \
UNION \
SELECT 'AT&T Center' Arena,'San Antonio' City,count(*) FROM nba WHERE upper(text) LIKE '%SAN ANTONIO%' or text like '%san antonio%' \
UNION \
SELECT 'Little Caesars Arena' Arena,'Detroit' City,count(*) FROM nba WHERE upper(text) LIKE '%DETROIT%' or text like '%detroit%' \
UNION \
SELECT 'Chase Center' Arena,'San Francisco' City,count(*) FROM nba WHERE upper(text) LIKE '%SAN FRANCISCO%' or text like '%san francisco%' \
UNION \
SELECT 'Talking Stick Resort Arena' Arena,'Phoenix' City,count(*) FROM nba WHERE upper(text) LIKE '%PHOENIX%' or text like '%phoenix%' \
UNION \
SELECT 'United Center' Arena,'Chicago' City,count(*) FROM nba WHERE upper(text) LIKE '%CHICAGO%' or text like '%chicago%'")
sqldf.show(150)

sqldf.toPandas().to_csv('2.csv')

#Pie Chart Code
df1 =  pd.read_csv('2.csv')
arena_data = df1["Arena"]
tournament_data = df1["City"]
count_data = df1["count(1)"]
colors = ["#ffdf22","#82cafc","#ff000d","#107ab0","#06c2ac","#beae8a","#f9bc08","#fd5956","#90b134","#01a049","#276ab3","#6c3461","#ff5b00","#a9561e"]
#explode = (0.1, 0, 0, 0, 0)
total_count= (sum(count_data))
data=[]
handles = []
for i, l  in enumerate(tournament_data):
    handles.append(matplotlib.patches.Patch(color=colors[i]))
    data.append(arena_data[i]+"-"+tournament_data[i]+"-"+str("{0:.2f}".format(float(count_data[i]*100)/total_count))+"%")



plt.legend(handles,data, bbox_to_anchor=(0.85,1.025), loc="upper left")
plt.pie(count_data, colors=colors, shadow=False, startangle=90)
plt.title("NBA 2019 - Number of matches being conducted in various cities")
plt.show()