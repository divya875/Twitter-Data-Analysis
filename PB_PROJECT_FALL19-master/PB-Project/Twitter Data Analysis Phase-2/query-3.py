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
import matplotlib.pyplot as plt
import matplotlib.patches
import pandas as pd
datetime_obj = datetime.datetime.strptime('Fri Nov 8 5:33:48 +0000 2019', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Fri Nov 8 5:33:48 +0000 2019','%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
df.createOrReplaceTempView("nba")
sqldf= spark.sql("SELECT nba.lang Language,count(*) Tweets  FROM nba WHERE nba.lang is NOT NULL  GROUP BY nba.lang ORDER BY 2 DESC limit 20")
sqldf.show(150)

sqldf.toPandas().to_csv('3.csv')
#Code for bar graph
data = pd.read_csv('3.csv')

plt.bar(data['Language'],data['Tweets'])
#data.plot.bar(x='loc',y='number_of_tweets')
plt.ylabel('Tweets')
plt.xlabel('Language')
plt.title('Tweets from top 20 languages')
plt.xticks(fontsize=7, rotation=30)
plt.yticks(fontsize=7)
plt.show()