
import dlt

# sales Expectations

sales_rules = {
    "rule1": "sales_id is not null"
}


# Empty Streaming table
dlt.create_streaming_table(
    name='stg_sales',
    expect_all_or_drop=sales_rules
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

