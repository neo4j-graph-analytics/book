# // tag::imports[]
import pandas as pd
from graphframes import *
from pyspark.sql.types import *
# // end::imports[]

from scripts.path_finding.shortestpath.custom_shortestpath import shortest_path
from scripts.path_finding.import_graph import create_transport_graph

# // tag::load-graph-frame[]
g = create_transport_graph()
# // end::load-graph-frame[]



# // tag::custom-shortest-path-execute[]
result = shortest_path(g, "Amsterdam", "Colchester", "cost")
result.select("id", "distance", "path").show(truncate=False)
# // end::custom-shortest-path-execute[]
