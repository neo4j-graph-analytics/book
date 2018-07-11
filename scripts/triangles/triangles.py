
# // tag::imports[]
from graphframes import *
from pyspark import SparkContext, SQLContext
# // end::imports[]

# // tag::sqlcontext[]
sqlContext = SQLContext(sc)
# // end::sqlcontext[]

# // tag::load-graph-frame[]
v = sqlContext.read.csv("data/friends-nodes.csv", header=True)
e = sqlContext.read.csv("data/friends-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::triangles[]
results = g.triangleCount()
results.sort("count", ascending=False).show()
# // end::triangles[]