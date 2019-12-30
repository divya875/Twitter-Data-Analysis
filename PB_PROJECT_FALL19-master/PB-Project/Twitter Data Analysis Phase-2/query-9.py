import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd
import requests
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("HashtagCount")\
        .getOrCreate()
    df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
    words = df.select(
        explode(
            split(df.text, " ")
        ).alias("word")
    )
    def extract_tags(word):
        if word.lower().startswith("#"):
            return word
        else:
            return "nonTag"
    extract_tags_udf = udf(extract_tags, StringType())
    resultDF = words.withColumn("tags", extract_tags_udf(words.word))
    resultDF.createOrReplaceTempView("hashtag_count")
    sqlhash = spark.sql("SELECT Hashtag,\
    	Occurrences\
    	FROM (SELECT upper(tags) Hashtag,\
    	count(*) Occurrences\
    	FROM hashtag_count\
    	WHERE 1=1\
    	AND tags!='nonTag'\
    	GROUP BY upper(tags)\
    	ORDER BY Occurrences desc, Hashtag asc) limit 5")
    sqlhash.show(70)

    sqlhash.toPandas().to_csv('9.csv')

#Code for bar graph
data = pd.read_csv('9.csv')
#plt.bar(data['player'],data['count'])
data.plot(kind="bar",x='Hashtag',y='Occurrences',legend=None)
#plt.legend().remove()
plt.ylabel('Occurrences')
plt.xlabel('Hashtags')
plt.title('Top 5 Hashags of NBA')
plt.xticks(fontsize=5, rotation=30)
plt.yticks(fontsize=5)
plt.show()