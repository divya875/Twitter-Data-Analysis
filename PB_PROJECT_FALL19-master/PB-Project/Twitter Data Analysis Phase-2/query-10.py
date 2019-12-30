from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import matplotlib.pyplot as plt
import pandas as pd
spark = SparkSession \
    .builder \
    .appName("Player recognition in NBA") \
    .getOrCreate()
# spark is an existing SparkSession
df = spark.read.json("C:/Users/Vamsi Draksharam/PycharmProjects/PB-Vamsi/phase2/data2.json")
# Displays the content of the DataFrame to stdout

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("nba")
sqlDF = spark.sql("SELECT 'Jordan Clarkson' as Player, count(*) as Count from nba where text like '%jordan%' and text like '%nba%'\
        UNION\
        SELECT 'Stephen Curry' as Player, count(*) as Count from nba where text like '%curry%' and text like '%nba%'\
        UNION\
        SELECT 'LeBron James' as Player, count(*) as Count from nba where text like '%lebron%' and text like '%nba%'  UNION\
        SELECT 'James Harden' as Player, count(*) as Count from nba where text like '%harden%' and text like '%nba%'  UNION\
        SELECT 'Anthony Davis' as Player, count(*) as Count from nba where text like '%anthony%' and text like '%nba%'")
pd = sqlDF.toPandas()
pd.to_csv('10.csv', index=False)
pd.plot(kind="bar",x="Player",y="Count")
plt.show()
sqlDF.show()