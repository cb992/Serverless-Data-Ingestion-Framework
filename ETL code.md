# Using pyspark for performing ETL operation!
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

#d2=d1.fillna(0,subset=['cancellation_code'])
d2=df.withColumn("CANCELLATION_CODE", when(df.CANCELLATION_CODE !="", df.CANCELLATION_CODE).otherwise('F'))

#d2=df
d2=d2.withColumn('Status', when(d2.ARR_DELAY >=1, 1).otherwise(0))

#dropping unnecessary columns from dataframe 
columns_to_drop = ['op_carrier_fl_num',
'dep_time',
'dep_delay',
'crs_arr_time',
'arr_time',
'cancelled',
'diverted',
'crs_elapsed_time',
'actual_elapsed_time',
'distance',
'late_aircraft_delay',
'Unnamed: 27'
]

```
