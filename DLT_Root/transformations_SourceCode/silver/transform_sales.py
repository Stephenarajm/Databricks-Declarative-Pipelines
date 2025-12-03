import dlt


#tansforming sales data

@dlt.view


# Create destination silver table
dlt.create_streaming_table(
    name='sales_enrich',
    comment='Enriched sales data'
)

dlt.create_auto_cdc_flow(
    target = "sales_enrich",
    source = "stg_sales",
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



