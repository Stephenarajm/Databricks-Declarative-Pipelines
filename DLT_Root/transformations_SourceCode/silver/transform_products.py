import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import *


@dlt.view(
    name = 'products_enrich_view'
)

def products_enrich_view():
    df = spark.readStream.table("stg_products")
    df = df.withColumn("price",col('price').cast('integer'))
    return df

# Create destination silver table

dlt.create_streaming_table(
    name='products_enrich'
)

dlt.create_auto_cdc_flow(
    target='products_enrich',
    source='products_enrich_view',
    keys = ["product_id"],
    sequence_by = "last_updated",
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


