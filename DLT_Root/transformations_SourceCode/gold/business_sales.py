import dlt

from pyspark.sql.functions import sum

# Creating a Materialized Nusiness view

@dlt.table(
    name='business_sales'
)

def business_sales():

    df_fact= spark.read.table("fact_sales")
    df_dim_customers= spark.read.table("dim_customers")
    df_dim_products= spark.read.table("dim_products")
    

    df_join = df_fact.join(df_dim_customers, df_fact.customer_id == df_dim_customers.customer_id, "inner") \
        .join(df_dim_products, df_fact.product_id == df_dim_products.product_id, "inner")
    
    df_prun = df_join.select("region","category","total_amount")

    df_agg = df_prun.groupBy("region","category").agg(sum("total_amount").alias("total_revenue"))

    return df_agg





