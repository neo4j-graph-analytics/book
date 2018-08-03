# // tag::imports[]
from pyspark.sql.types import *
from graphframes import *
from pyspark.sql import functions as F
import pandas as pd
# // end::imports[]

from scripts.path_finding.sssp.custom_sssp import sssp

# // tag::load-graph-frame[]
fields = [
    StructField("id", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("population", IntegerType(), True)
]
v = spark.read.csv("data/transport-nodes.csv", header=True, schema=StructType(fields))

src_dst = spark.read.csv("data/transport-relationships.csv", header=True)
df_src_dst = src_dst.toPandas()
df_dst_src = src_dst.toPandas()
df_dst_src.columns = ["dst", "src", "relationship", "cost"]
e = spark.createDataFrame(pd.concat([df_src_dst, df_dst_src]))

g = GraphFrame(v, e)
# // end::load-graph-frame[]

# // tag::via[]
via_udf = F.udf(lambda path: path[1:-1], ArrayType(StringType()))
# // end::via[]

# // tag::execute[]
result = sssp(g, "Amsterdam", "cost")
result \
    .withColumn("via", via_udf("path")) \
    .select("id", "distance", "via") \
    .sort("distance") \
    .show(truncate=False)
# // end::execute[]
