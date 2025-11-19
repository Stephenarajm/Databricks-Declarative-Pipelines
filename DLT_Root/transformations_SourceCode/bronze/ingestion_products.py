
import dlt

# Products Expectations

product_rules = {
    "rule1": "product_id is not null",
    "rule2": "price >= 0"
}

# Ingesting products
@dlt.table(
    name='stg_products'
)

@dlt.expect_all(product_rules)

def stg_products():
    df=spark.readStream.table('dbtstev.source.products')
    return df