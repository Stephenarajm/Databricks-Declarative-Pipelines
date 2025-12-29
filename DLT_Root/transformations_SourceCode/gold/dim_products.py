import dlt


# Create Empty streaming table
dlt.create_streaming_table(
    name='dim_products'
)


# AUTO CDC FLOW

dlt.create_auto_cdc_flow(
    target='dim_products',
    source=''
)