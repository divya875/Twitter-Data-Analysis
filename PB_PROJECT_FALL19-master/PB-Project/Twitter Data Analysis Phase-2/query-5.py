from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import matplotlib.pyplot as plt
import pandas as pd
spark = SparkSession \
    .builder \
    .appName("List of top players and their occurrences") \
    .getOrCreate()
# spark is an existing SparkSession
df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")

df.createOrReplaceTempView("NBA")
sqlDF = spark.sql("SELECT 'Chris Paul' as Player, count(*) as Occurrences from nba where text like '%chris paul%' or text like '%nba%' or upper(text) like '%CHRIS PAUL%' or upper(text) like '%NBA%'\
        UNION\
        SELECT 'Stephen Curry' as Player, count(*) as Occurrences from nba where text like '%curry%' or upper(text) like '%CURRY%'\
        UNION\
        SELECT 'Kevin Durant' as Player, count(*) as Occurrences from nba where text like '%kevin durant%' or upper(text) like '%KEVIN DURANT%' or text like '%nba%' or upper(text) like '%NBA%'  UNION\
        SELECT 'LeBron James' as Player, count(*) as Occurrences from nba where text like '%lebron%' or upper(text) like '%LEBRON%' or text like '%LeBron James'  UNION\
        SELECT 'Russell Westbrook' as Player, count(*) as Occurrences from nba where text like '%westbrook%' or upper(text) like '%WESTBROOK%'")
pd = sqlDF.toPandas()
pd.to_csv('5.csv', index=False)
#code of bar-graph
pd.plot(kind="bar",x="Player",y="Occurrences")
plt.show()
sqlDF.show()