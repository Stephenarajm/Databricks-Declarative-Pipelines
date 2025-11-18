
import dlt


# Empty Streaming table
dlt.create_streaming_table(
    name='stg_sales'
)

# --Creating east sales flow

@dlt.append_flow(target='stg_sales')

def east_sales():
    df = spark.read.table('dbtstev.source.sales_east')
    return df


# --Creating west sales flow

@dlt.append_flow(target='stg_sales')

def west_sales():
    df = spark.read.table('dbtstev.source.sales_west')
    return df

