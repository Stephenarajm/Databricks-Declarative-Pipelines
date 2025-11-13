import dlt

# Creating streaming tables

@dlt.table(
    name='first_stream_table'
)
def first_stream_table():
  
  df = spark.readStream.table('dbtstev.source.orders')
  return df


# Create Materialized View
@dlt.table(
  name='first_mat_view'
)

def first_mat_view():
  df = spark.read.table('dbtstev.source.orders')
  return df

# Create View

@dlt.view(
  name='first_view'
)
def first_view():
  df = spark.read.table('dbtstev.source.orders')  
  return df




