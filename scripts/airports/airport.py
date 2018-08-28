# // tag::imports[]
from graphframes import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pandas as pd

# // end::imports[]

# // tag::load-graph-frame[]
nodes = spark.read.csv("data/airports.csv", header=False)

cleaned_nodes = (nodes.select("_c1", "_c3", "_c4")
                 .filter("_c3 = 'United States'")
                 .withColumnRenamed("_c1", "name")
                 .withColumnRenamed("_c4", "id")
                 .drop("_c3"))

relationships = spark.read.csv("data/188591317_T_ONTIME.csv", header=True)

cleaned_relationships = (relationships
                         .select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY",
                                 "DISTANCE", "TAIL_NUM", "FL_NUM", "CRS_DEP_TIME")
                         .withColumnRenamed("ORIGIN", "src")
                         .withColumnRenamed("DEST", "dst")
                         .withColumnRenamed("DEP_DELAY", "deptDelay")
                         .withColumnRenamed("ARR_DELAY", "arrDelay")
                         .withColumnRenamed("TAIL_NUM", "tailNumber")
                         .withColumnRenamed("FL_NUM", "flightNumber")
                         .withColumnRenamed("FL_DATE", "date")
                         .withColumnRenamed("CRS_DEP_TIME", "time")
                         .withColumnRenamed("DISTANCE", "distance")
                         .withColumn("deptDelay", F.col("deptDelay").cast(FloatType()))
                         .withColumn("arrDelay", F.col("arrDelay").cast(FloatType()))
                         .withColumn("time", F.col("time").cast(IntegerType()))
                         )

g = GraphFrame(cleaned_nodes, cleaned_relationships)
# // end::load-graph-frame[]

# // tag::nodes[]
g.vertices.count()
# // end::nodes[]

# // tag::relationships[]
g.edges.count()
# // end::relationships[]

# // tag::longest-departing-delay[]
g.edges.groupBy().max("deptDelay").show()
# // end::longest-departing-delay[]

# // tag::ord-delays[]
result = (g.edges
 .filter("src = 'ORD' and deptDelay > 0")
 .groupBy("src", "dst")
 .agg(F.avg("deptDelay"), F.count("deptDelay"))
 .sort(F.desc("avg(deptDelay)")))

(result
 .join(g.vertices, result.dst == g.vertices.id)
 .withColumn("averageDelay", F.round(F.col("avg(deptDelay)"), 2))
 .withColumn("numberOfDelays", F.col("count(deptDelay)"))
 .select("dst", "name", "averageDelay", "numberOfDelays")
 .show(n=10, truncate=False))

# // end::ord-delays[]

# tag::ord-ckb[]
from_expr = 'id = "ORD"'
to_expr = 'id = "CKB"'
result = g.bfs(from_expr, to_expr)

(result
 .select(F.col("e0.date"),
         F.col("e0.time"),
         F.col("e0.flightNumber"),
         F.col("e0.deptDelay"))
 .sort("deptDelay", ascending=False)
 .show(n=50))

# end::ord-ckb[]


# // tag::motifs-delayed-flights[]
motifs = (g.find("(a)-[ab]->(b); (b)-[bc]->(c)")
          .filter("""(b.id = 'SFO') and 
                     (ab.arrDelay > 500 or bc.deptDelay > 500) and
                     (ab.tailNumber = bc.tailNumber) and 
                     (ab.date = bc.date) and 
                     (ab.time < bc.time)"""))


# // end::motifs-delayed-flights[]

# // tag::motifs-udf[]
def sum_dist(dist1, dist2):
    return sum([value for value in [dist1, dist2] if value is not None])


sum_dist_udf = F.udf(sum_dist, FloatType())
# // end::motifs-udf[]


# // tag::motifs-delayed-flights-result[]
result = (motifs.withColumn("delay",
                            sum_dist_udf(motifs.ab.arrDelay, motifs.bc.deptDelay))
          .select("ab", "bc", "delay")
          .sort("delay", ascending=False))

result.select(
    F.col("ab.src").alias("airport1"),
    F.col("ab.arrDelay"),
    F.col("ab.dst").alias("airport2"),
    F.col("bc.deptDelay"),
    F.col("bc.dst").alias("airport3"),
    F.col("delay")
).show()
# // end::motifs-delayed-flights-result[]


# tag::pagerank[]
result = g.pageRank(resetProbability=0.15, maxIter=20)
(result.vertices
 .sort("pagerank", ascending=False)
 .withColumn("pagerank", F.round(F.col("pagerank"), 2))
 .show(truncate=False))
# end::pagerank[]


# tag::triangles[]
triangles = g.triangleCount().cache()
pagerank = g.pageRank(resetProbability=0.15, maxIter=20).cache()

(triangles.select(F.col("id").alias("tId"), "count")
 .join(pagerank.vertices, F.col("tId") == F.col("id"))
 .select("id", "name", "pagerank", "count")
 .sort("count", ascending=False)
 .withColumn("pagerank", F.round(F.col("pagerank"), 2))
 .show(truncate=False))

# end::triangles[]