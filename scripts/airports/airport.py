# // tag::imports[]
from graphframes import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# // end::imports[]

# tag::matplotlib-imports[]
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
# end::matplotlib-imports[]

# // tag::load-graph-frame[]
nodes = spark.read.csv("data/airports.csv", header=False)

cleaned_nodes = (nodes.select("_c1", "_c3", "_c4", "_c6", "_c7")
                 .filter("_c3 = 'United States'")
                 .withColumnRenamed("_c1", "name")
                 .withColumnRenamed("_c4", "id")
                 .withColumnRenamed("_c6", "latitude")
                 .withColumnRenamed("_c7", "longitude")
                 .drop("_c3"))
cleaned_nodes = cleaned_nodes[cleaned_nodes["id"] != "\\N"]

relationships = spark.read.csv("data/188591317_T_ONTIME.csv", header=True)

cleaned_relationships = (relationships
                         .select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY",
                                 "DISTANCE", "TAIL_NUM", "FL_NUM", "CRS_DEP_TIME",
                                 "CRS_ARR_TIME","UNIQUE_CARRIER")
                         .withColumnRenamed("ORIGIN", "src")
                         .withColumnRenamed("DEST", "dst")
                         .withColumnRenamed("DEP_DELAY", "deptDelay")
                         .withColumnRenamed("ARR_DELAY", "arrDelay")
                         .withColumnRenamed("TAIL_NUM", "tailNumber")
                         .withColumnRenamed("FL_NUM", "flightNumber")
                         .withColumnRenamed("FL_DATE", "date")
                         .withColumnRenamed("CRS_DEP_TIME", "time")
                         .withColumnRenamed("CRS_ARR_TIME", "arrivalTime")
                         .withColumnRenamed("DISTANCE", "distance")
                         .withColumnRenamed("UNIQUE_CARRIER", "airline")
                         .withColumn("deptDelay", F.col("deptDelay").cast(FloatType()))
                         .withColumn("arrDelay", F.col("arrDelay").cast(FloatType()))
                         .withColumn("time", F.col("time").cast(IntegerType()))
                         .withColumn("arrivalTime", F.col("arrivalTime").cast(IntegerType()))
                         )

g = GraphFrame(cleaned_nodes, cleaned_relationships)
# // end::load-graph-frame[]

# tag::airlines-mapping-csv[]
airlines_reference = (spark.read.csv("data/airlines.csv")
      .select("_c1", "_c3")
      .withColumnRenamed("_c1", "name")
      .withColumnRenamed("_c3", "code"))

airlines_reference = airlines_reference[airlines_reference["code"] != "null"]

# end::airlines-mapping-csv[]

# tag::airlines-mapping-json[]
df = spark.read.option("multiline", "true").json("data/airlines.json")
dummyDf = spark.createDataFrame([("test", "test")], ["code", "name"])

for code in df.schema.fieldNames():
    tempDf = (df.withColumn("code", F.lit(code))
              .withColumn("name", df[code]))
    tdf = tempDf.select("code", "name")
    dummyDf = dummyDf.union(tdf)
# end::airlines-mapping-json[]

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
airports_degree = g.outDegrees.withColumnRenamed("id", "oId")

full_airports_degree = (airports_degree
                        .join(g.vertices, airports_degree.oId == g.vertices.id)
                        .sort("outDegree", ascending=False)
                        .select("id", "name", "outDegree"))

full_airports_degree.show(n=10, truncate=False)
# // end::flight-count[]

# // tag::flight-plot[]
plt.style.use('fivethirtyeight')

ax = (full_airports_degree
 .toPandas()
 .head(10)
 .plot(kind='bar', x='id', y='outDegree', legend=None))

ax.xaxis.set_label_text("")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
# end::flight-plot[]

plt.savefig("/tmp/airports.svg")
plt.close()

# // tag::ord-delays[]
delayed_flights = (g.edges
 .filter("src = 'ORD' and deptDelay > 0")
 .groupBy("dst")
 .agg(F.avg("deptDelay"), F.count("deptDelay"))
 .withColumn("averageDelay", F.round(F.col("avg(deptDelay)"), 2))
 .withColumn("numberOfDelays", F.col("count(deptDelay)")))

(delayed_flights
 .join(g.vertices, delayed_flights.dst == g.vertices.id)
 .sort(F.desc("averageDelay"))
 .select("dst", "name", "averageDelay", "numberOfDelays")
 .show(n=10, truncate=False))

# // end::ord-delays[]

# tag::ord-ckb[]
from_expr = 'id = "ORD"'
to_expr = 'id = "CKB"'
ord_to_ckb = g.bfs(from_expr, to_expr)

ord_to_ckb = ord_to_ckb.select(
  F.col("e0.date"),
  F.col("e0.time"),
  F.col("e0.flightNumber"),
  F.col("e0.deptDelay"))
# end::ord-ckb[]

# tag::ord-ckb-plot[]
ax = (ord_to_ckb
 .sort("date")
 .toPandas()
 .plot(kind='bar', x='date', y='deptDelay', legend=None))

ax.xaxis.set_label_text("")
plt.tight_layout()
plt.show()
# end::ord-ckb-plot[]

plt.savefig("/tmp/ord-ckb.svg")
plt.close()


# // tag::motifs-delayed-flights[]
motifs = (g.find("(a)-[ab]->(b); (b)-[bc]->(c)")
          .filter("""(b.id = 'SFO') and
                  (ab.date = '2018-05-11' and bc.date = '2018-05-11') and
                  (ab.arrDelay > 30 or bc.deptDelay > 30) and
                  (ab.flightNumber = bc.flightNumber) and
                  (ab.airline = bc.airline) and
                  (ab.time < bc.time)"""))
# // end::motifs-delayed-flights[]

# // tag::motifs-udf[]
def sum_dist(dist1, dist2):
    return sum([value for value in [dist1, dist2] if value is not None])


sum_dist_udf = F.udf(sum_dist, FloatType())
# // end::motifs-udf[]

# // tag::motifs-delayed-flights-result[]
result = (motifs.withColumn("delta", motifs.bc.deptDelay - motifs.ab.arrDelay)
          .select("ab", "bc", "delta")
          .sort("delta", ascending=False))

result.select(
    F.col("ab.src").alias("a1"),
    F.col("ab.time").alias("a1DeptTime"),
    F.col("ab.arrDelay"),
    F.col("ab.dst").alias("a2"),
    F.col("bc.time").alias("a2DeptTime"),
    F.col("bc.deptDelay"),
    F.col("bc.dst").alias("a3"),
    F.col("ab.airline"),
    F.col("ab.flightNumber"),
    F.col("delta")
).show()
# // end::motifs-delayed-flights-result[]



# tag::pagerank[]
result = g.pageRank(resetProbability=0.15, maxIter=20)
(result.vertices
 .sort("pagerank", ascending=False)
 .withColumn("pagerank", F.round(F.col("pagerank"), 2))
 .show(truncate=False, n=100))
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

clusters = airline_graph.labelPropagation(maxIter=10)
(clusters
 .sort("label")
 .groupby("label")
 .agg(F.collect_list("id").alias("airports"),
      F.count("id").alias("count"))
 .sort("count", ascending=False)
 .show(truncate=70, n=10))

# end::airport-clusters[]

# tag::airport-clusters-drilldown[]
all_flights = g.degrees.withColumnRenamed("id", "aId")
# end::airport-clusters-drilldown[]

# tag::airport-clusters-drilldown1[]
(clusters
 .filter("label=1606317768706")
 .join(all_flights, all_flights.aId == result.id)
 .sort("degree", ascending=False)
 .select("id", "name", "degree")
 .show(truncate=False))
# end::airport-clusters-drilldown1[]

# tag::airport-clusters-drilldown2[]
(clusters
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

full_name_airlines = (airlines_reference
                      .join(airlines, airlines.airline == airlines_reference.code)
                      .select("code", "name", "flights"))
# end::airlines[]

# tag::airlines-plot[]
ax = (full_name_airlines.toPandas()
      .plot(kind='bar', x='name', y='flights', legend=None))

ax.xaxis.set_label_text("")
plt.tight_layout()
plt.show()
# end::airlines-plot[]

plt.savefig("/tmp/airlines-count.svg")
plt.close()

# tag::scc-airlines-fn[]
def find_scc_components(g, airline):
    # Create a sub graph containing only flights on the provided airline
    airline_relationships = g.edges[g.edges.airline == airline]
    airline_graph = GraphFrame(g.vertices, airline_relationships)

    # Calculate the Strongly Connected Components
    scc = airline_graph.stronglyConnectedComponents(maxIter=10)

    # Find the size of the biggest component and return that
    return (scc
        .groupBy("component")
        .agg(F.count("id").alias("size"))
        .sort("size", ascending=False)
        .take(1)[0]["size"])
# end::scc-airlines-fn[]

# tag::scc-airlines[]
# Calculate the largest Strongly Connected Component for each airline
airline_scc = [(airline, find_scc_components(g, airline))
               for airline in airlines.toPandas()["airline"].tolist()]
airline_scc_df = spark.createDataFrame(airline_scc, ['id', 'sccCount'])

# Join the SCC DataFrame with the airlines DataFrame so that we can show the number of flights
# an airline has alongside the number of airports reachable in its biggest component
airline_reach = (airline_scc_df
 .join(full_name_airlines, full_name_airlines.code == airline_scc_df.id)
 .select("code", "name", "flights", "sccCount")
 .sort("sccCount", ascending=False))
# end::scc-airlines[]

# tag::scc-airlines-plot[]
ax = (airline_reach.toPandas()
      .plot(kind='bar', x='name', y='sccCount', legend=None))

ax.xaxis.set_label_text("")
plt.tight_layout()
plt.show()
# end::scc-airlines-plot[]

plt.savefig("/tmp/airlines-scc-count.svg")
plt.close()

# tag::dump-to-csv[]
(clusters
 .filter("label=1606317768706")
 .coalesce(1)
 .write
 .format("csv")
 .save("/tmp/foo6"))
# end::dump-to-csv[]

# tag::bfs-experimentation[]
filtered_rels = g.edges.filter("date = '2018-05-27'")
g2 = GraphFrame(g.vertices, filtered_rels)

res = g2.bfs("id='JFK'", "id='WYS'")

(res
 .filter("e0.arrivalTime < e1.time")
 .select(F.col("e0.airline"),
         F.col("e0.flightNumber"),
         F.col("e0.time"),
         F.col("v1.name"),
         F.col("e1.airline"),
         F.col("e1.flightNumber"),
         F.col("e1.time"))
 .show(truncate=False))

g3 = GraphFrame(g.vertices.filter("id <> 'SLC'"), g2.edges)

res = g3.bfs("id='JFK'", "id='WYS'")

(res
 .filter("e0.arrivalTime < e1.time")
 .select(F.col("e0.airline"),
         F.col("e0.flightNumber"),
         F.col("e0.time"),
         F.col("v1.name"),
         F.col("e1.airline"),
         F.col("e1.flightNumber"),
         F.col("e1.time"))
 .show(truncate=False))
# end::bfs-experimentation[]
