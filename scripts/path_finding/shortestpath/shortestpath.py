# // tag::imports[]
import pandas as pd
from graphframes import *
from pyspark.sql.types import *
# // end::imports[]

from scripts.path_finding.shortestpath.custom_shortestpath import dijkstra

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


# // tag::custom-shortest-path-execute[]
result = dijkstra(g, "Amsterdam", "Colchester")
result.select("id", "distance", "path").show(truncate=False)
# // end::custom-shortest-path-execute[]
