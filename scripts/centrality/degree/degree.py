
# // tag::imports[]
from graphframes import *
from pyspark import SparkContext
# // end::imports[]

# // tag::load-graph-frame[]
v = spark.read.csv("data/social-nodes.csv", header=True)
e = spark.read.csv("data/social-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::degree[]
total_degree = g.degrees
in_degree = g.inDegrees
out_degree = g.outDegrees

total_degree.join(in_degree, "id", how="left") \
            .join(out_degree, "id", how="left") \
            .fillna(0) \
            .sort("inDegree", ascending=False) \
            .show()
# // end::degree[]
