from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#creating a spark Session
def Spark_Session():
    spark=SparkSession.builder\
    .appName("Spark Assigment 1").getOrCreate()
    return spark
#creating a data frame which has conetnts of user.csv file
def user_DataFrame(spark):
    user_df=spark.read.csv("../resource/user.csv",header="true",inferSchema="true")
    return user_df

#creating a data frame which has conetnts of transaction.csv file
def transaction_DataFrame(spark):
    transaction_df=spark.read.csv("../resource/transaction.csv",header="true",inferSchema="true")
    return transaction_df

# joining the transaction and user data frames using inner join
def join_DataFrame(user_df,transaction_df):

    join_df=transaction_df.join(user_df,user_df.user_id \
            ==  transaction_df.userid,"inner")
    return join_df

#count of unique locations with product sold in that location
def unique_loc_count(join_df):
    loc_count=join_df.groupby("location","product_description").\
        agg(count("location").alias("count_unique_loc"))
    return loc_count

#products bought by each user
def user_prod(join_df):
    user_bought=join_df.groupby("userid").\
        agg(count("product_description").alias("product_count"))
    return user_bought

#total spending done by each user on each product.
def tot_spend(join_df):
    total_spend=join_df.groupby("userid","product_id").\
        agg(sum("price").alias("total_spend"))
    return total_spend