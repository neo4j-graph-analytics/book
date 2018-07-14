
# // tag::imports[]
from graphframes import *
from pyspark import SparkContext, SQLContext
# // end::imports[]

# // tag::load-graph-frame[]
v = spark.read.csv("data/sw-nodes.csv", header=True)
e = spark.read.csv("data/sw-relationships.csv", header=True)
g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::lpa[]
from pyspark.sql import functions as F

result = g.labelPropagation(maxIter=10)
result.sort("label").groupby("label").agg(F.collect_list("id")).show(truncate=False)
# // end::lpa[]