from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "card_transactions remove duplicates")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

Df = spark.read.csv("C:/Users/rpravesh/Desktop/card_transactions.csv")
print(Df.distinct().count())
Df.show()

df1 = Df.select('_c0','_c5')
print(df1.distinct().count())

df2=Df.dropDuplicates(["_c0","_c5"])
print(df2.distinct().count())
df2.show()

df2.coalesce(1).write\
    .format("csv")\
    .mode('overwrite')\
    .save("C:/Users/rpravesh/Desktop/credit-card-fraud-detection/project_input_data")

spark.stop()
