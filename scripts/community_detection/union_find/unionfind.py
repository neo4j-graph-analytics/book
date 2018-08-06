# // tag::imports[]
from pyspark.sql import functions as F
# // end::imports[]

from scripts.community_detection.import_graph import  create_software_graph

# // tag::load-graph-frame[]
g = create_software_graph()
# // end::load-graph-frame[]

sc.setCheckpointDir("/tmp")

# // tag::unionfind[]
result = g.connectedComponents()
result.sort("component") \
    .groupby("component") \
    .agg(F.collect_list("id").alias("libraries")) \
    .show(truncate=False)
#  // end::unionfind[]