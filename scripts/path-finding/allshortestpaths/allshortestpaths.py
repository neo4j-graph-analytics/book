# // tag::imports[]
from graphframes import *
from pyspark import SparkContext
# // end::imports[]

# // tag::load-graph-frame[]
v = spark.read.csv("data/transport-nodes.csv", header=True)
e = spark.read.csv("data/transport-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::allshortestpaths[]
result = g.shortestPaths(list(g.vertices.toPandas()["id"]))
result.sort(["id"]).show()
# // end::allshortestpaths[]

# // tag::allshortestpaths-more[]
df = result.toPandas()
df.set_index("id", inplace=True)

routes = [(location, d, dest[d])
 for location, dest in df.to_records()
 for d in dest
]

for start, end, distance in routes[0:10]:
    print(start, end, distance)
# // end::allshortestpaths-more[]
