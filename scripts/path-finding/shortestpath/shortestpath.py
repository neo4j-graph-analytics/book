# // tag::imports[]
import pandas as pd
from graphframes import *
from pyspark.sql.types import *

# // end::imports[]

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

# // tag::custom-shortest-path-imports[]
from scripts.aggregate_messages import AggregateMessages as AM
from pyspark.sql import functions as F


# // end::custom-shortest-path-imports[]

# // tag::custom-shortest-path[]
def dijkstra(g, origin):
    vertices = g.vertices \
        .withColumn("visited", F.lit(False)) \
        .withColumn("distance", F.when(g.vertices["id"] == origin, 0).otherwise(float("inf")))
    cached_vertices = AM.getCachedDataFrame(vertices)
    g2 = GraphFrame(cached_vertices, g.edges)
    for i in range(1, g.vertices.count() - 1):
        current_node_id = g2.vertices.filter('visited == False').sort("distance").first().id
        msg_for_dst = F.when(AM.src['id'] == current_node_id, AM.edge['cost'] + AM.src['distance'])
        new_distances = g2.aggregateMessages(F.min(AM.msg).alias("aggMess"), sendToDst=msg_for_dst)

        new_visited_col = F.when(g2.vertices.visited | (g2.vertices.id == current_node_id), True).otherwise(False)
        new_distance_col = F.least(g2.vertices.distance, new_distances.aggMess)
        new_vertices = g2.vertices.join(new_distances, on="id", how="left_outer") \
            .drop(new_distances["id"]) \
            .withColumn("visited", new_visited_col) \
            .withColumn("newDistance", new_distance_col) \
            .drop("aggMess") \
            .drop('distance') \
            .withColumnRenamed('newDistance', 'distance')
        cached_new_vertices = AM.getCachedDataFrame(new_vertices)
        
        g2 = GraphFrame(cached_new_vertices, g2.edges)
    return GraphFrame(g2.vertices.drop("visited"), g2.edges)
# // end::custom-shortest-path[]

# // tag::shortestpath[]
result = dijkstra(g, "London")
# // end::shortestpath[]

# // tag::shortestpath-columns[]
print(result.columns)
# // end::shortestpath-columns[]


#  // tag::shortestpath-results[]
columns = [column for column in result.columns if not column.startswith("e")]
result.select(columns).show()
#  // end::shortestpath-results[]
