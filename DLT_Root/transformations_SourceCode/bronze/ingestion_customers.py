
import dlt


# Customers Expectations

customer_rules = {
    "rule1": "customer_id is not null",
    "rule2": "customer_name is not null"
}


# Ingesting customers
@dlt.table(
    name='stg_customers'
)

@dlt.expect_all_or_drop(customer_rules)

def stg_customers():
    df=spark.readStream.table('dbtstev.source.customers')
    return df