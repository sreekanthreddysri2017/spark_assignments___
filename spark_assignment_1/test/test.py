from pyspark.sql.types import *
import unittest
from pyspark.sql import SparkSession
from Spark_assign_1.core.util import *


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("PySpark-unit-test")
                     .config('spark.port.maxRetries', 30)
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_something(self):
        user_Schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location", StringType(), True)
        ])
        user_Data = [(101, "abc.123 @ gmail.com", "hindi", "mumbai"),
                     (102, "abc.123 @ gmail.com", "hindi", "usa"),
                     (103,"madan.44@gmail.com","marathi","nagpur"),
                     (104, "local.88 @ outlook.com", "tamil", "chennai")
                  ]
        user_df = self.spark.createDataFrame(data=user_Data, schema=user_Schema)

        transactionSchema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True)
        ])
        transaction_Data = [(3300101,1000001,101,700,"mouse"),
                            (3300102,1000002,102,900,"laptop"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge")

                            ]
        transaction_df = self.spark.createDataFrame(data=transaction_Data, schema=transactionSchema)


        expected_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True)

        ])

        expected_data = [(101, "abc.123 @ gmail.com", "hindi", "mumbai",3300104,1000004,101,35000,"fridge"),
                         (101, "abc.123 @ gmail.com", "hindi", "mumbai",3300101, 1000001, 101, 700, "mouse"),
                         (102, "abc.123 @ gmail.com", "hindi", "usa",3300102,1000002,102,900,"laptop"),
                         (103,"madan.44@gmail.com","marathi","nagpur",3300103,1000003,103,34000,"tv")]


        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        transformed_df = join_DataFrame(user_df,transaction_df)
        transformed_df.show()


        expected_schema1 = StructType([

            StructField("product_description", StringType(), True),
            StructField("location", StringType(), True),
            StructField("new_count", LongType(), True)
        ])
        expected_data1 = [("mouse","mumbai",1),
                          ("laptop","usa",1),
                          ("tv","nagpur",1),
                          ("fridge","mumbai",1)]
        expected_df1 = self.spark.createDataFrame(data=expected_data1, schema=expected_schema1)
        transformed_df1 = unique_loc_count(transformed_df)
        transformed_df1.show()


        expected_schema2 = StructType([

            StructField("userid", IntegerType(), True),
            StructField("new_count", LongType(), True)
        ])
        expected_data2 = [(101, 2), (102,1),(103,1) ]
        expected_df2 = self.spark.createDataFrame(data=expected_data2, schema=expected_schema2)
        transformed_df2 = user_prod(transformed_df)
        transformed_df2.show()




        expected_schema3 = StructType([

            StructField("userid", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("Total_Amount", LongType(), True)
        ])
        expected_data3 = [(101,"fridge", 35000), (101,"mouse",700), (102,"laptop",900),(103,"tv",34000)]
        expected_df3 = self.spark.createDataFrame(data=expected_data3, schema=expected_schema3)
        transformed_df3 = tot_spend(transformed_df)
        transformed_df3.show()



if __name__ == '__main__':
    unittest.main()