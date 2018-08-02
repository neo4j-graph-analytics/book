# // tag::imports[]
import pandas as pd
from graphframes import *
from pyspark.sql.types import *
# // end::imports[]


# // tag::custom-shortest-path-imports[]
from scripts.aggregate_messages import AggregateMessages as AM
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

# // end::custom-shortest-path-imports[]

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


# // tag::udfs[]
def add_path(s, s2):
    return s + [s2]

add_path_udf = udf(add_path, ArrayType(StringType()))
# // end::udfs[]


# // tag::custom-shortest-path[]
def dijkstra(g, origin, destination, column_name="cost"):
    if g.vertices.filter(g.vertices.id == destination).count() == 0:
        return spark \
            .createDataFrame(sc.emptyRDD(), g.vertices.schema) \
            .withColumn("path", F.array())

    vertices = g.vertices \
        .withColumn("visited", F.lit(False)) \
        .withColumn("distance",
            F.when(g.vertices["id"] == origin, 0).otherwise(float("inf"))) \
        .withColumn("path", F.array())
    cached_vertices = AM.getCachedDataFrame(vertices)
    g2 = GraphFrame(cached_vertices, g.edges)

    while g2.vertices.filter('visited == False').first():
        current_node_id = g2.vertices.filter('visited == False').sort("distance").first().id

        msg = F.struct(AM.edge[column_name] + AM.src['distance'],
                       add_path_udf(AM.src["path"], AM.src["id"]))
        msg_for_dst = F.when(AM.src['id'] == current_node_id, msg)

        new_distances = g2.aggregateMessages(F.min(AM.msg).alias("aggMess"),
                                             sendToDst=msg_for_dst)
        new_visited_col = F.when(g2.vertices.visited | (g2.vertices.id == current_node_id), \
                                 True) \
                           .otherwise(False)
        new_distance_col = F.when(new_distances["aggMess"].isNotNull() &
                                  (new_distances.aggMess["col1"] < g2.vertices.distance),
                                  new_distances.aggMess["col1"]) \
                            .otherwise(g2.vertices.distance)
        new_path_col = F.when(new_distances["aggMess"].isNotNull() &
                              (new_distances.aggMess["col1"] < g2.vertices.distance),
                              new_distances.aggMess["col2"].cast("array<string>")) \
                        .otherwise(g2.vertices.path)
        new_vertices = g2.vertices.join(new_distances, on="id", how="left_outer") \
            .drop(new_distances["id"]) \
            .withColumn("visited", new_visited_col) \
            .withColumn("newDistance", new_distance_col) \
            .withColumn("newPath", new_path_col) \
            .drop("aggMess", "distance", "path") \
            .withColumnRenamed('newDistance', 'distance') \
            .withColumnRenamed('newPath', 'path')
        cached_new_vertices = AM.getCachedDataFrame(new_vertices)
        g2 = GraphFrame(cached_new_vertices, g2.edges)
        if g2.vertices.filter(g2.vertices.id == destination).first().visited:
            return g2.vertices \
                .filter(g2.vertices.id == destination) \
                .drop("visited") \
                .withColumn("newPath", add_path_udf("path", "id")) \
                .drop("path") \
                .withColumnRenamed("newPath", "path")
    return spark \
        .createDataFrame(sc.emptyRDD(), g.vertices.schema) \
        .withColumn("path", F.array())
# // end::custom-shortest-path[]


# // tag::custom-shortest-path-execute[]
result = dijkstra(g, "Amsterdam", "London")
result.select("id", "distance", "path").show(truncate=False)
# // end::custom-shortest-path-execute[]
