# import dlt
# from pyspark.sql.functions import *

# '''Creating End-to-End basic pipeline'''

# # Staging Area

# @dlt.table(
#     name="staging_orders"
# )

# def staging_orders():

#     df=spark.readStream.table("dbtstev.source.orders")
#     return(df)


# # Creating transformed area

# @dlt.view(
#     name="transformed_orders"
# )

# def transformed_orders():

#     df=spark.readStream.table("staging_orders")
#     df=df.withColumn("order_status",upper(df.order_date))
#     # df=df.filter(df.order_status != "completed")
#     return(df)


# # Creating Aggregated Areas

# @dlt.table(
#     name="aggregated_orders"
# )

# def aggregated_orders():
#     df=spark.readStream.table("transformed_orders")
#     df=df.groupBy("order_status").count()
#     return(df)


