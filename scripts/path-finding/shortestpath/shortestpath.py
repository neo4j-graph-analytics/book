
# // tag::imports[]
from graphframes import *
import pandas as pd
# // end::imports[]

# // tag::load-graph-frame[]
v = spark.read.csv("data/transport-nodes.csv", header=True)

src_dst = spark.read.csv("data/transport-relationships.csv", header=True)
df_src_dst = src_dst.toPandas()
df_dst_src = src_dst.toPandas()
df_dst_src.columns = ["dst", "src", "relationship", "cost"]
e = spark.createDataFrame(pd.concat([df_src_dst, df_dst_src]))

g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::shortestpath[]
result = g.bfs("id='Amsterdam'", "id='London'")
columns = [column for column in result.columns
           if not column.startswith("e")]

result.select(columns).show()
# // end::shortestpath[]
