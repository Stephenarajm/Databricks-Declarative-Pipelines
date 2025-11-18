
import dlt


# Ingesting products
@dlt.table(
    name='stg_products'
)

def stg_products():
    df=spark.readStream.table('dbtstev.source.products')
    return df