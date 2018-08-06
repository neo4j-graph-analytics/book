# // tag::imports[]
from graphframes import *
# // end::imports[]


# // tag::load-graph-frame[]
def create_software_graph():
    nodes = spark.read.csv("data/sw-nodes.csv", header=True)
    relationships = spark.read.csv("data/sw-relationships.csv", header=True)
    return GraphFrame(nodes, relationships)
# // end::load-graph-frame[]
