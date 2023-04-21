from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def Spark_Session():
    spark=SparkSession.builder\
          .appName("Spark_Assignment_2").getOrCreate()
    return spark

def create_torrent_DataFrame(spark):
    ghtorrent_Schema=StructType([StructField("Log_level",StringType(),True),
                               StructField("timeStamp",StringType(),True),
                               StructField("gh_torrent_details",StringType(),True)])
    ghtorrent_df=spark.read.option("header",True).schema(ghtorrent_Schema).csv("../resource/ghtorrent-logs.txt")
    return ghtorrent_df

def ghtorrent_fields(ghtorrent_df):
    ghtorrent_extract=ghtorrent_df.withColumn("ghTorrent",split(col("gh_torrent_details"),"--").getItem(0))\
        .withColumn("ghTorrent_remaining",split(col("gh_torrent_details"),"--").getItem(1))\
        .withColumn("client_details",split(col("ghTorrent_remaining"),":").getItem(0))\
        .withColumn("url",split(col("ghTorrent_remaining"),":").getItem(1))
    return ghtorrent_extract

def total_line(df):
    c = df.agg(count("*").alias("count_of_lines"))
    return c

def warn_count(df):
    countOfWarn=df.filter(col("Log_level")=="WARN").agg(count("*").alias("warning_count"))
    return countOfWarn

def api_client_repo(df):
    api_client_repo_processed=df.filter(col("client_details").like("%api_client%"))\
                              .agg(count("*").alias("api_client_repo_processed"))
    return api_client_repo_processed

def most_http(df):
    total_http_client=df.filter(col("url").like("%https%"))\
                      .groupby("ghtorrent").agg(count("*").alias("most_http"))
    count_most_http=total_http_client.sort(col("most_http").desc())
    return count_most_http

def failed_request(df):

    failed_http_client = df.filter(col("url").like("%Failed%")) \
        .groupby("ghtorrent").agg(count("*").alias("most_http"))
    most_failed_http = failed_http_client.sort(col("most_http").desc())
    return most_failed_http


def active_hour(df):
    hour_active=df.withColumn("active_hour",hour(col("timeStamp"))).groupby("active_hour")\
                .agg(count("*").alias("most_active_hour"))
    most_hour_active=hour_active.sort(col("active_hour").desc())
    return most_hour_active

def active_repo(df):
    active_repository=df.groupby("ghtorrent")\
                        .agg(count("*").alias("most_active_repository"))
    active_repository.sort("most_active_repository")
    return active_repository