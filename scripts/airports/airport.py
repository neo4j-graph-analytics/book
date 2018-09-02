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
cleaned_nodes = cleaned_nodes[cleaned_nodes["id"] != "\\N"]

relationships = spark.read.csv("data/188591317_T_ONTIME.csv", header=True)

cleaned_relationships = (relationships
                         .select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY",
                                 "DISTANCE", "TAIL_NUM", "FL_NUM", "CRS_DEP_TIME",
                                 "UNIQUE_CARRIER")
                         .withColumnRenamed("ORIGIN", "src")
                         .withColumnRenamed("DEST", "dst")
                         .withColumnRenamed("DEP_DELAY", "deptDelay")
                         .withColumnRenamed("ARR_DELAY", "arrDelay")
                         .withColumnRenamed("TAIL_NUM", "tailNumber")
                         .withColumnRenamed("FL_NUM", "flightNumber")
                         .withColumnRenamed("FL_DATE", "date")
                         .withColumnRenamed("CRS_DEP_TIME", "time")
                         .withColumnRenamed("DISTANCE", "distance")
                         .withColumnRenamed("UNIQUE_CARRIER", "airline")
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

# // tag::flight-count[]
all_flights = g.outDegrees.withColumnRenamed("id", "oId")

(all_flights
 .join(g.vertices, all_flights.oId == g.vertices.id)
 .sort("outDegree", ascending=False)
 .select("id", "name", "outDegree")
 .show(n=10, truncate=False))
# // end::flight-count[]

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

(result.select(
    F.col("e0.date"),
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
    F.col("ab.date"),
    F.col("ab.src").alias("a1"),
    F.col("ab.time").alias("a1DeptTime"),
    F.col("ab.arrDelay"),
    F.col("ab.dst").alias("a2"),
    F.col("bc.time").alias("a2DeptTime"),
    F.col("bc.deptDelay"),
    F.col("bc.dst").alias("a3"),
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

# tag::airport-clusters[]

airline_relationships = g.edges.filter("airline = 'DL'")
airline_graph = GraphFrame(g.vertices, airline_relationships)

result = airline_graph.labelPropagation(maxIter=10)
(result
 .sort("label")
 .groupby("label")
 .agg(F.collect_list("id").alias("airports"),
      F.count("id").alias("count"))
 .sort("count", ascending=False)
 .show(truncate=70))

# end::airport-clusters[]

# tag::airport-clusters-drilldown[]
all_flights = g.degrees.withColumnRenamed("id", "aId")
# end::airport-clusters-drilldown[]

# tag::airport-clusters-drilldown1[]
(result
 .filter("label=1606317768706")
 .join(all_flights, all_flights.aId == result.id)
 .sort("degree", ascending=False)
 .select("id", "name", "degree")
 .show(truncate=False))
# end::airport-clusters-drilldown1[]

# tag::airport-clusters-drilldown2[]
(result
 .filter("label=1219770712067")
 .join(all_flights, all_flights.aId == result.id)
 .sort("degree", ascending=False)
 .select("id", "name", "degree")
 .show(truncate=False))
# end::airport-clusters-drilldown2[]


# tag::airlines[]
airlines = (g.edges
 .groupBy("airline")
 .agg(F.count("airline").alias("flights"))
 .sort("flights", ascending=False))

airlines.show()
# end::airlines[]


# tag::scc-airlines-fn[]
def find_scc_components(g, airline):
    airline_relationships = g.edges[g.edges.airline == airline]
    airline_graph = GraphFrame(g.vertices, airline_relationships)
    result = airline_graph.stronglyConnectedComponents(maxIter=10)
    return (result
        .groupBy("component")
        .agg(F.count("id").alias("size"))
        .sort("size", ascending=False)
        .take(1)[0]["size"])
# end::scc-airlines-fn[]

scc_udf = F.udf(lambda airline: find_scc_components(airline), IntegerType())
(airlines.withColumn("sccCount", scc_udf(g, airlines.airline)))

# tag::scc-airlines[]
airline_scc = [(airline, find_scc_components(g, airline))
               for airline in airlines.toPandas()["airline"].tolist()]

airline_scc_df = spark.createDataFrame(airline_scc, ['id', 'sccCount'])
(airline_scc_df
 .join(airlines, airlines.airline == airline_scc_df.id)
 .select("id", "flights", "sccCount")
 .sort("sccCount", ascending=False)
 .show())
# end::scc-airlines[]
