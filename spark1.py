from pyspark.sql.functions import col, desc, asc, round, corr, countDistinct, min, max, sum, count, avg, expr, to_date, year
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from _functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import isnan
from pyspark.sql import functions as F

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

import sys
input_path = sys.argv[1]

input_bucket = 's3://kafka-dbstream/'
#input_path = 'topics/dbstream.public.orders/year=*/month=*/day=*/hour=*/*.avro'
output_path = 's3://kafka-dbstream/Outputs/ordersmerge'
input_path_merge = 'Outputs/ordersmerge/*'

df = spark.read.format("avro").load(input_bucket + input_path)

df.createOrReplaceTempView("cdctemp")

tempview = spark.sql("""WITH change_data AS (
    SELECT (CASE WHEN op = 'd' THEN before.order_id ELSE after.order_id END) order_id, after.customer_id as customer_id,after.total_price as total_price,after.menu_id as menu_id, after.restraunt_id as restraunt_id,after.feedback as feedback,after.created_at as created_at, op, ts_ms from
	cdctemp
	)

SELECT
    order_id, customer_id, total_price, menu_id, restraunt_id, feedback, created_at from (
    SELECT *, ROW_NUMBER() OVER
        (PARTITION BY order_id 
         ORDER BY ts_ms DESC) AS n
	FROM change_data ) WHERE n = 1
	AND op <> 'd'
    """)

df1 = spark.read.format("parquet").load(input_bucket + input_path_merge)


output2 = df1.join(tempview, ['order_id','customer_id','total_price','menu_id','restraunt_id','feedback','created_at'],how="outer")

output2.createOrReplaceTempView("outputtable")

spark.sql("CACHE table outputtable")
output2.count()

output2.write.mode("overwrite").format("parquet").save(output_path)