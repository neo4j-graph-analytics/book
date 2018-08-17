# // tag::imports[]
from graphframes import *
from pyspark.sql.types import *
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
  .select("ORIGIN", "DEST", "DEP_DELAY", "ARR_DELAY", "DISTANCE", "TAIL_NUM", "FL_NUM")
  .withColumnRenamed("ORIGIN", "src")
  .withColumnRenamed("DEST", "dst")
  .withColumnRenamed("DEP_DELAY", "departureDelay")
  .withColumnRenamed("ARR_DELAY", "arrivalDelay")
  .withColumnRenamed("TAIL_NUM", "tailNumber")
  .withColumnRenamed("FL_NUM", "flightNumber")
)

g = GraphFrame(cleaned_nodes, cleaned_relationships)
# // end::load-graph-frame[]


from_expr = "id='ORD'"
to_expr = "id='SFO'"
result = g.bfs(from_expr, to_expr)