
# // tag::imports[]
from graphframes import *
from pyspark.sql import functions as F
# // end::imports[]

# // tag::load-graph-frame[]
v = spark.read.csv("data/sw-nodes.csv", header=True)
e = spark.read.csv("data/sw-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::scc[]
result = g.stronglyConnectedComponents(maxIter=10)
result.sort("component") \
    .groupby("component") \
    .agg(F.collect_list("id").alias("libraries")) \
    .show(truncate=False)
# // end::scc[]
