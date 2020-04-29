import os
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, countDistinct, col
from pyspark.sql.types import StructType, StringType
from normalizer import normalize_string, validate_date, validate_ip_address

kafka_url = "localhost:9092"
default_topic_name = 'topic1'

os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 pyspark-shell"
)

# If there are multiple Java installations, should be set here for runtime;
# os.environ["JAVA_HOME"] = '/Library/Java/JavaVirtualMachines/jdk1.8.0_221.jdk/Contents/Home/'

parser = argparse.ArgumentParser()
parser.add_argument('--topic', nargs='?', type=str, help='name of the topic', default=default_topic_name)


def get_schema():
    return StructType() \
        .add("id", StringType()) \
        .add("first_name", StringType()) \
        .add("last_name", StringType()) \
        .add("email", StringType()) \
        .add("gender", StringType()) \
        .add("ip_address", StringType()) \
        .add("date", StringType()) \
        .add("country", StringType())


def get_read_stream(spark):
    args = parser.parse_args()

    PERSON_EVENTS = get_schema()
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json").cast("string"), PERSON_EVENTS).alias("parsed_value")) \
        .select("parsed_value.*")

    df = normalize_fields(df)
    # df.printSchema()
    return df


def start_spark_session():
    spark = SparkSession \
        .builder \
        .appName("app1") \
        .master("local") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def normalize_fields(df):
    df = df.withColumn("country", normalize_string("country"))
    df = df.withColumn("ip_address", validate_ip_address("ip_address"))
    df = df.withColumn("date", validate_date("date"))
    return df


def foreach_batch_function(df, epoch_id):
    df.groupBy("country").count().orderBy("count", ascending=False) \
        .select(col("country").alias("maxCountry"), col("count")).limit(1).show()

    df.groupBy("country").count().orderBy("count", ascending=True) \
        .select(col("country").alias("minCountry"), col("count")).limit(1).show()

    df.agg(countDistinct(col("first_name")).alias("numberOfUniqueUsers")).show()


def listen_stream(spark, df):
    df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("numRows", "500") \
        .start()

    df.writeStream.foreachBatch(foreach_batch_function).start()
    spark.streams.awaitAnyTermination()


def consume():
    spark = start_spark_session()
    df = get_read_stream(spark)
    listen_stream(spark, df)


if __name__ == '__main__':
    consume()
