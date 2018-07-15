# // tag::imports[]
from graphframes import *
from pyspark import SparkContext, SQLContext
# // end::imports[]

# // tag::load-graph-frame[]
v = spark.read.csv("data/transport-nodes.csv", header=True)
e = spark.read.csv("data/transport-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::allshortestpaths[]
result = g.shortestPaths(list(g.vertices.toPandas()["id"]))
result.sort(["id", "distances"]).show(truncate=False)
# // end::allshortestpaths[]