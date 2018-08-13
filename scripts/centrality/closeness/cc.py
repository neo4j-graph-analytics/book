from graphframes import *

# // tag::imports[]
from scripts.aggregate_messages import AggregateMessages as AM
from pyspark.sql import functions as F
from pyspark.sql.types import *
from operator import itemgetter


# // end::imports[]


# // tag::udfs[]
def collect_paths(paths):
    return F.collect_set(paths)


collect_paths_udf = F.udf(collect_paths, ArrayType(StringType()))

paths_type = ArrayType(StructType([
    StructField("id", StringType()),
    StructField("distance", IntegerType())
]))


def flatten(ids):
    flat_list = [item for sublist in ids for item in sublist]
    return list(dict(sorted(flat_list, key=itemgetter(0))).items())


flatten_udf = F.udf(flatten, paths_type)


def new_paths(paths, id):
    paths = [{"id": col1, "distance": col2 + 1} for col1, col2 in paths if col1 != id]
    paths.append({"id": id, "distance": 1})
    return paths


new_paths_udf = F.udf(new_paths, paths_type)


def merge_paths(ids, new_ids, id):
    joined_ids = ids + (new_ids if new_ids else [])
    merged_ids = [(col1, col2) for col1, col2 in joined_ids if col1 != id]
    best_ids = dict(sorted(merged_ids, key=itemgetter(1), reverse=True))
    return [{"id": col1, "distance": col2} for col1, col2 in best_ids.items()]


merge_paths_udf = F.udf(merge_paths, paths_type)


def calculate_closeness(ids):
    nodes = len(ids)
    total_distance = sum([col2 for col1, col2 in ids])
    return 0 if total_distance == 0 else nodes * 1.0 / total_distance


closeness_udf = F.udf(calculate_closeness, DoubleType())
# // end::udfs[]

# // tag::closeness[]
vertices = g.vertices.withColumn("ids", F.array())
cached_vertices = AM.getCachedDataFrame(vertices)
g2 = GraphFrame(cached_vertices, g.edges)

for i in range(0, g2.vertices.count()):
    msg_dst = new_paths_udf(AM.src["ids"], AM.src["id"])
    msg_src = new_paths_udf(AM.dst["ids"], AM.dst["id"])
    agg = g2.aggregateMessages(F.collect_set(AM.msg).alias("agg"),
        sendToSrc=msg_src, sendToDst=msg_dst)
    res = agg.withColumn("newIds", flatten_udf("agg")).drop("agg")
    new_vertices = g2.vertices.join(res, on="id", how="left_outer") \
        .withColumn("mergedIds", merge_paths_udf("ids", "newIds", "id")) \
        .drop("ids", "newIds") \
        .withColumnRenamed("mergedIds", "ids")
    cached_new_vertices = AM.getCachedDataFrame(new_vertices)
    g2 = GraphFrame(cached_new_vertices, g2.edges)

g2.vertices \
    .withColumn("closeness", closeness_udf("ids")) \
    .sort("closeness", ascending=False) \
    .show(truncate=False)
# // end::closeness[]
