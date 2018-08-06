
# // tag::imports[]
from pyspark.sql import functions as F
# // end::imports[]

from scripts.community_detection.import_graph import  create_software_graph

# // tag::load-graph-frame[]
g = create_software_graph()
# // end::load-graph-frame[]

# // tag::lpa[]
result = g.labelPropagation(maxIter=10)
result.sort("label").groupby("label").agg(F.collect_list("id")).show(truncate=False)
# // end::lpa[]
