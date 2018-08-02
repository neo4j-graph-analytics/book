# // tag::imports[]
from graphframes import *

# // end::imports[]

# // tag::load-graph-frame[]
v = spark.read.csv("data/sw-nodes.csv", header=True)
e = spark.read.csv("data/sw-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::triangles[]
result = g.triangleCount()
result.sort("count", ascending=False) \
    .filter('count > 0') \
    .show()
#  // end::triangles[]
