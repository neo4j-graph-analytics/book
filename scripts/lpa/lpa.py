
# // tag::imports[]
from graphframes import *
from pyspark import SparkContext, SQLContext
# // end::imports[]

# // tag::sqlcontext[]
sqlContext = SQLContext(sc)
# // end::sqlcontext[]

# // tag::load-graph-frame[]
v = sqlContext.read.csv("data/social-nodes.csv", header=True)
e = sqlContext.read.csv("data/social-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::lpa[]
result = g.labelPropagation(maxIter=10)
result.sort("label").show()
# // end::lpa[]