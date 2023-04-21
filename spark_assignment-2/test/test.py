from Spark_assign_2.core.util import *
import unittest
from pyspark.sql.types import *
from pyspark.sql.functions import *

class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .appName("Spark_Assignment_2")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


def logfile(spark):
    logfile_Schema = StructType([
        StructField("Logging",StringType(),True),
        StructField("timestamp",StringType(),True),
        StructField("ghtorrent_id",StringType(),True)
          ])
    data = [("DEBUG", "2017-03-23T11:15:14+00:00",
             "ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
            ("DEBUG", "2017-03-23T11:15:14+00:00",
             "ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
            ("INFO", "2017-03-23T11:08:13+00:00",
             "ghtorrent-8 -- api_client.rb: Successful request. URL: https://api.github.com/repos/Particular/NServiceBus.Persistence.ServiceFabric/pulls/10/comments?per_page=100, Remaining: 3333, Total: 110 ms"),
            ("DEBUG", "2017-03-23T12:02:06+00:00",
             "ghtorrent-31 -- ghtorrent.rb: Association of commit 7ce873512d609ed53a6c38eb32d5a1a706712c0d with repo ovh/cds exists"),
            ("INFO", "2017-03-23T09:11:17+00:00",
             "ghtorrent-5 -- api_client.rb: Successful request. URL: https://api.github.com/repos/javier-serrano/web-ui-a2?per_page=100, Remaining: 3381, Total: 153 ms"),
            ("WARN", "2017-03-23T10:33:43+00:00",
             "ghtorrent-23 -- ghtorrent.rb: Not a valid email address: greenkeeper[bot]")]
    sparkDF = spark.createDataFrame(data,logfile_Schema)

#def Withcolumn_Log(sparkDF):

    split_Column=sparkDF\
            .withColumn("ghtorrent",split(col("ghtorrent_id"),"--").getItem(0))\
            .withColumn("ghtorrent_remain",split(col("ghtorrent_id"),"--").getItem(1))\
            .withColumn("api_client",split(col("ghtorrent_remain"),":").getItem(0)) \
            .withColumn("url", split(col("ghtorrent_remain"), ":").getItem(1))

    split_Column.show()

def warn_count(df):
    find_warn_log = df.filter(col("Log_level") == "WARN")\
         .agg(count("*").alias("warn_count"))
    return find_warn_log

def total_line(df):
    total_line_count = df.agg(count("*")
                              .alias("total_line"))
    return total_line_count

def api_client_repo(df):
    api_Client_count = df.filter(col("api_client")\
                       .like("%api_client%")).agg(count("*").alias("Total_api_Client"))
    return api_Client_count

def most_http(df):
    most_Http_count=df.groupBy("ghtorrent")\
        .agg(count("ghtorrent")
             .alias("Most_Http"))
    most_Http_count.sort(col("Most_Http").desc())
    return most_Http_count

def failed_request(df):
    failed_Request_count = df.filter(col("url")
        .like("%Failed%"))\
        .agg(count("*")
        .alias("failed_Request_count"))
    return failed_Request_count

def active_hour(df):
    most_Active_Hour_Count = df.withColumn("active_hour",hour(col("timestamp")))\
             .groupBy("active_hour")\
             .agg(count("*")
             .alias("moar_Active_Hour"))
    return most_Active_Hour_Count

def active_repo(df):
    active_Repository = df.groupBy("ghtorrent")\
        .agg(count("*").alias("most_Active_Repository"))
    active_Repository.sort("most_Active_Repository")
    return active_Repository.sort(col("Most_Http").desc())

if __name__ == '__main__':
    unittest.main()