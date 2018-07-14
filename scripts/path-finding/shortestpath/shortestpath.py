
# // tag::imports[]
from graphframes import *
from pyspark import SparkContext, SQLContext
# // end::imports[]

# // tag::sqlcontext[]
sqlContext = SQLContext(sc)
# // end::sqlcontext[]

# // tag::load-graph-frame[]
v = sqlContext.read.csv("data/transport-nodes.csv", header=True)
e = sqlContext.read.csv("data/transport-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::shortestpath[]
result = g.bfs("id='A'", "id='F'")
result.show()
# // end::shortestpath[]