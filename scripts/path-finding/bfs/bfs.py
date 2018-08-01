# // tag::imports[]
from graphframes import *
from pyspark.sql.types import *
import pandas as pd

# // end::imports[]

# // tag::load-graph-frame[]
fields = [
    StructField("id", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("population", IntegerType(), True)
]
v = spark.read.csv("data/transport-nodes.csv", header=True, schema=StructType(fields))

src_dst = spark.read.csv("data/transport-relationships.csv", header=True)
df_src_dst = src_dst.toPandas()
df_dst_src = src_dst.toPandas()
df_dst_src.columns = ["dst", "src", "relationship", "cost"]

e = spark.createDataFrame(pd.concat([df_src_dst, df_dst_src]))

g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::candidates[]
g.vertices \
    .filter("population > 100000 and population < 300000") \
    .sort("population") \
    .show()
# // end::candidates[]


# // tag::shortestpath[]
from_expr = "id='Den Haag'"
to_expr = "population > 100000 and population < 300000 and id <> 'Den Haag'"
result = g.bfs(from_expr, to_expr)
# // end::shortestpath[]

# // tag::shortestpath-columns[]
print(result.columns)
# // end::shortestpath-columns[]


#  // tag::shortestpath-results[]
columns = [column for column in result.columns if not column.startswith("e")]
result.select(columns).show()
#  // end::shortestpath-results[]
