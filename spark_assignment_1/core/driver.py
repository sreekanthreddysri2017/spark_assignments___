from Spark_assign_1.core.util import *

#creatng a spark session
spark=Spark_Session()

trans_df=transaction_DataFrame(spark)
print("transaction table")
trans_df.show()

user_df = user_DataFrame(spark)
print("user table")
user_df.show()

join_df=join_DataFrame(user_df,trans_df)
print("tables after joining")
join_df.show()
#joining transaction and user dataframes
join_df=join_DataFrame(spark)
print("tables after joining")
join_df.show()

#finding the count of unique location with product sold in that particular location
unique_location=unique_loc_count(join_df)
print("count of unique locations with product sold in that location")
unique_location.show()

#finding the products bought by each user
prod_bought_user=user_prod(join_df)
print("products bought by each user")
prod_bought_user.show()

#finding the total amount of spending by each user on each product
exp_user_prod=tot_spend(join_df)
print("total spending done by each user on each product")
exp_user_prod.show()