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

datetime_obj = datetime.datetime.strptime('Fri Nov 8 7:48:12 +0000 2019', '%a %b %d %H:%M:%S +0000 %Y')
print(type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Fri Nov 8 7:48:12 +0000 2019', '%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))
spark = SparkSession \
    .builder \
    .appName("HashtagCount") \
    .getOrCreate()
df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
df.createOrReplaceTempView("nba")
sqldf = spark.sql(
    "SELECT name,SUM(cnt) as retweet FROM (SELECT quoted_status.user.screen_name AS name,quoted_status.retweet_count AS cnt FROM nba WHERE quoted_status.retweet_count>0)GROUP BY name ORDER BY retweet DESC LIMIT 15")
sqldf.show(150)

sqldf.toPandas().to_csv('8.csv')
# Pie-chart Code
data = pd.read_csv('8.csv')

tournament_data = data["name"]
count_data = data["retweet"]
colors = ["#ffdf22","#840000","#ccad60","#048243","#b25f03","#606602","#82cafc","#ff000d","#107ab0","#06c2ac","#beae8a","#f9bc08","#fd5956","#90b134","#01a049"]
#explode = (0.1, 0, 0, 0, 0)
total_count= (sum(count_data))
data=[]
handles = []
for i, l  in enumerate(tournament_data):
    handles.append(matplotlib.patches.Patch(color=colors[i]))
    data.append(tournament_data[i]+"-"+str("{0:.2f}".format(float(count_data[i]*100)/total_count))+"%")



plt.legend(handles,data, bbox_to_anchor=(0.85,1.025), loc="upper left")
plt.pie(count_data, colors=colors, shadow=False, startangle=90)
plt.title("Count of retweets ")
plt.show()