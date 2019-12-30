import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt
import requests
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("NBA Top Players list")\
        .getOrCreate()
    df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
    
    df.createOrReplaceTempView("NBA")
    sqlhash = spark.sql("SELECT 'Quinn' player,count(text) as count  \
        FROM NBA\
        WHERE 1=1\
        AND (upper(text) LIKE '%COOK%' or upper(text) LIKE '%QUINN%' or upper(text) LIKE '%QUI%')\
        GROUP BY player\
        UNION\
        SELECT 'Klay' player,count(text) as count \
        FROM NBA\
        WHERE 1=1\
        AND (upper(text) LIKE '%KLAY%' or upper(text) LIKE '%THOMPSON%')\
        GROUP BY player\
        UNION\
        SELECT 'Stephen' player,count(text) as count \
        FROM NBA\
        WHERE 1=1\
        AND (upper(text) LIKE '%STEPHEN%' or text LIKE '%stephen%')\
        GROUP BY player\
        UNION\
        SELECT 'Draymond' player,count(text) as count\
        FROM NBA\
        WHERE 1=1\
        AND (upper(text) LIKE '%DRAYMOND%' or upper(text) LIKE '%GREEN%')\
        GROUP BY player\
        UNION\
        SELECT 'DAMIAN' player,count(text) as count \
        FROM NBA\
        WHERE 1=1\
        AND (upper(text) LIKE '%DAMIAN%' or text LIKE '%damian%')\
        GROUP BY player\
        UNION\
        SELECT 'Jordan' player,count(text) as count \
        FROM NBA\
        WHERE 1=1\
        AND (upper(text) LIKE '%JORDAN BELL%' or upper(text) LIKE '%JORDAN%' or upper(text) LIKE '%BELL%')\
        GROUP BY player")
    sqlhash.show()
    sqlhash.toPandas().to_csv('1.csv')

#Code for bar graph
data = pd.read_csv('1.csv')
data.plot(kind="bar",x='player',y='count',legend=None)
plt.ylabel('Votes')
plt.xlabel('Player Namer')
plt.title('NBA Trending Players-2019')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()