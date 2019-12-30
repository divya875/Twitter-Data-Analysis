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
import sys, tweepy, csv, re
from textblob import TextBlob
import matplotlib.pyplot as plt
import matplotlib.patches
import pandas as pd

datetime_obj = datetime.datetime.strptime('Fri Nov 8 5:49:11 +0000 2019', '%a %b %d %H:%M:%S +0000 %Y')
print(type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Fri Nov 8 5:49:11 +0000 2019', '%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))
spark = SparkSession \
    .builder \
    .appName("HashtagCount") \
    .getOrCreate()
df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
df.createOrReplaceTempView("Users")
sqldf = spark.sql(
    "SELECT user.id,user.name,count(*) FROM Users"
    " WHERE (user.id is not null and user.name is not null) group by user.id,user.name order by 3 desc limit 9")
sqldf.show(150)

sqldf.toPandas().to_csv('6.csv')
# Code for bar graph
data = pd.read_csv('6.csv')

plt.bar(data['name'], data['count(1)'])
plt.ylabel('count')
plt.xlabel('name')
plt.title('Tweets from top Users')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()