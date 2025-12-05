import dlt
from pyspark.sql.functions import col

#tansforming sales data

@dlt.view(
    name='stg_sales_transform'
)

def stg_sales_transform():
    df = spark.readStream.table("stg_sales")
    df = df.withColumn("total_amount",col("quantity") * col("amount"))  
    return df


# Create destination silver table
dlt.create_streaming_table(
    name='sales_enrich',
    comment='Enriched sales data'
)

dlt.create_auto_cdc_flow(
    target = "sales_enrich",
    source = "stg_sales_transform",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",
    # ignore_null_updates = <bool>,
    # apply_as_deletes = None,
    # apply_as_truncates = None,
    # column_list = None,
    # except_column_list = None,
    stored_as_scd_type = 1
    # track_history_column_list = None,
    # track_history_except_column_list = None,
    # name = None,
    # once = <bool>
)



