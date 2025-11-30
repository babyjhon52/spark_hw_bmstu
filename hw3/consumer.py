from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, StringType, LongType

spark = SparkSession.builder \
    .appName("consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

schema = StructType([
    StructField('timestamp', StringType()),
    StructField('stats', MapType(StringType(), LongType()))
])

df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'raw_messages') \
    .option('startingOffsets', 'latest') \
    .load()

parsed_df = df.select(
    from_json(col('value').cast('string'), schema).alias('data')
).select('data.*')

words_df = parsed_df.select(
    explode(col('stats')).alias('word', 'count')
)

aggregated_df = words_df.groupBy('word').agg(sum('count').alias('total_count'))

top10_df = aggregated_df.orderBy(col('total_count').desc()).limit(10)

query = top10_df.writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', 'false') \
    .trigger(processingTime='1 minute') \
    .start()

query.awaitTermination()
