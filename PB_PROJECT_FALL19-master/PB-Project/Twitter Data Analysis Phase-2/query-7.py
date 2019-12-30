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
datetime_obj = datetime.datetime.strptime('Fri Nov 8 4:46:48 +0000 2019', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Fri Nov 8 4:46:48 +0000 2019','%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
df.createOrReplaceTempView("nba")
sqldf = spark.sql("SELECT 'All-Star Game MVP' award,count(text) as count  \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%LEBRON JAMES%' or text like '%LeBron James%' or upper(text) LIKE '%LEBRON%' or text like '%LeBron%')\
    GROUP BY award\
    UNION\
    SELECT 'Rookie of the Year' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%STEPHEN%' or upper(text) LIKE '%CURRY%' or text like '%stephen%')\
    GROUP BY award     UNION\
    SELECT 'Most Valuable Player' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%KEVIN DURANT%' or text like '%kevin durant%' or text like '%Durant%')\
    GROUP BY award   UNION\
    SELECT 'Coach of the Year' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%ANTHONY DAVIS%' or text like '%anthony davis%')\
    GROUP BY award   UNION\
    SELECT 'NBA Finals Most Valuable Player' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%JAMES HARDEN%' or text like '%james harden%')\
    GROUP BY award   UNION\
    SELECT 'Executive of the Year' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%ANTETOKOUNMPO%' or text like '%antetokounmpo%')\
    GROUP BY award  UNION\
    SELECT 'Citizenship Award' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%EMBIID%' or text like '%Embiid%')\
    GROUP BY award  UNION\
    SELECT 'Defensive Player of the Year' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%RUSSELL WESTBROOK%' or text like '%Westbrook%')\
    GROUP BY award  UNION\
    SELECT 'Sixth Man of the Year' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%PAUL GEORGE%' or text like '%Paul George%')\
    GROUP BY award  UNION\
    SELECT 'Most Improved Player' award,count(text) as count \
    FROM nba\
    WHERE 1=1\
    AND (upper(text) LIKE '%KAWHI LEONARD%' or text like '%Kawhi Leonard%' or text like '%Kawhi%')\
    GROUP BY award")
sqldf.show(150)

sqldf.toPandas().to_csv('7.csv')

#Code for bar graph
data = pd.read_csv('7.csv')
data.plot(kind="bar",x='award',y='count',legend=None)
plt.ylabel('Popularity')
plt.xlabel('Name of the Award')
plt.title('Popular Awards of NBA')
plt.xticks(fontsize=6, rotation=40)
plt.yticks(fontsize=7)
plt.show()