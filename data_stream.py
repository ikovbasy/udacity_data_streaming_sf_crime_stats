import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
jsonSchema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),    
    StructField('call_date_time', StringType(), True),    
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),    
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "udacity.project2.police.calls_3") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    # Show schema for the incoming resources for checks
    #df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df1 = df.selectExpr("CAST(value AS string)")
    
#     kafka_df2 = kafka_df1 \
#         .select(psf.from_json('value', jsonSchema)


    #query = kafka_df2.writeStream \
        #.outputMode("append") \
        #.format('console') \
        #.start()
    
    #query.awaitTermination()

    # TODO select original_crime_type_name and disposition
    distinct_table = kafka_df1 \
        .select(psf.from_json('value', jsonSchema).alias("DF")) \
        .select(["DF.original_crime_type_name", "DF.disposition"])

    # count the number of original crime type
    agg_df = distinct_table.groupBy(["original_crime_type_name", "disposition"]).count()
    agg_df = agg_df.orderBy(psf.desc("count"))

#     # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
#     # TODO write output stream
#     query = agg_df \
#         .writeStream \
#         .outputMode("complete") \
#         .format("console") \
#         .start() \
    
#     # TODO attach a ProgressReporter
#     query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, 'disposition')
    
    join_query_stream = join_query \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start() \

    join_query_stream.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config('spark.ui.port', 3000) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
