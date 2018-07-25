
# // tag::imports[]
from graphframes import *
from pyspark import SparkContext
# // end::imports[]

# // tag::load-graph-frame[]
v = spark.read.csv("data/transport-nodes.csv", header=True)
e = spark.read.csv("data/transport-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::shortestpath[]
result = g.bfs("id='Amsterdam'", "id='London'")
columns = [column for column in result.columns
           if not column.startswith("e")]

result.select(columns).show()
# // end::shortestpath[]
