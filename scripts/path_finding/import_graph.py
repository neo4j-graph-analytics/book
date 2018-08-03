# // tag::imports[]
from pyspark.sql.types import *
from graphframes import *
from pyspark.sql import functions as F
import pandas as pd
# // end::imports[]


# // tag::load-graph-frame[]
def create_transport_graph():
    node_fields = [
        StructField("id", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("population", IntegerType(), True)
    ]
    nodes = spark.read.csv("data/transport-nodes.csv", header=True,
                           schema=StructType(node_fields))

    rels = spark.read.csv("data/transport-relationships.csv", header=True)
    reversed_rels = rels.withColumn("newSrc", rels.dst) \
        .withColumn("newDst", rels.src) \
        .drop("dst", "src") \
        .withColumnRenamed("newSrc", "src") \
        .withColumnRenamed("newDst", "dst") \
        .select("src", "dst", "relationship", "cost")

    relationships = rels.union(reversed_rels)

    return GraphFrame(nodes, relationships)
# // end::load-graph-frame[]
