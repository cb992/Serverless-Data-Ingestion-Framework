```
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, when
sc = SparkContext('local')
#spark = SparkSession(sc)
spark = SparkSession.builder.appName('abc').getOrCreate()
df=spark.read.csv("s3://project-24/temp/2018.csv",header=True)

```
