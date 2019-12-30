import datetime, pytz
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import sys,tweepy,csv,re
from textblob import TextBlob
spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
date= df.select("created_at")
def dateMTest(dateval):
    dt=datetime.datetime.strptime(dateval, '%a %b %d %H:%M:%S +0000 %Y')
    return dt
d = udf(dateMTest , DateType())
df=df.withColumn("created_date",d(date.created_at))
df.createOrReplaceTempView("nba")
sqldf= spark.sql("SELECT id,text,created_date  FROM nba WHERE 1=1 AND (upper(text) LIKE '%LEBRON%'AND text LIKE '%nba%')")
i=0
positive=0
neutral=0
negative=0
for t in sqldf.select("text").collect():
    i=i+1
    
    analysis = TextBlob(str((t.text).encode('ascii', 'ignore')))
    print(analysis.sentiment.polarity)
    if (analysis.sentiment.polarity<0):
       	negative=negative+1
       	print(i," in negative")
    elif(analysis.sentiment.polarity==0.0):
        neutral=neutral+1
        print(i," in neutral")
    elif(analysis.sentiment.polarity>0):
        positive=positive+1
        print(i," in positive")
print("The total negative percentage is",((negative)*100)/i)
print("The total neutral percentage is",((neutral)*100)/i)
print("The total positive percentage is",((positive)*100)/i)
percentage_of_negative_votes=((negative)*100)/i
percentage_of_positive_votes=((positive)*100)/i
percentage_of_neutral_votes=((neutral)*100)/i

#Code for Donut pie chart
size_of_groups=[percentage_of_negative_votes,percentage_of_positive_votes,percentage_of_neutral_votes]
names='percentage_of_negative_votes', 'percentage_of_positive_votes', 'percentage_of_neutral_votes'
# Create a pieplot
plt.pie(size_of_groups,labels=names, colors=['brown','yellow','grey'])

# Adding circle at center
my_circle=plt.Circle( (0,0), 0.3, color='white')
p=plt.gcf()
p.gca().add_artist(my_circle)
plt.title("LeBron James Supporters in NBA-2019")
plt.show()