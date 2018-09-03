# // tag::imports[]
from pyspark.sql.types import *
from graphframes import *
from pyspark.sql import functions as F
import pandas as pd
# // end::imports[]

from scripts.path_finding.sssp.custom_sssp import sssp
from scripts.path_finding.import_graph import create_transport_graph

# // tag::load-graph-frame[]
g = create_transport_graph()
# // end::load-graph-frame[]

# // tag::via[]
via_udf = F.udf(lambda path: path[1:-1], ArrayType(StringType()))
# // end::via[]

# // tag::execute[]
result = sssp(g, "Amsterdam", "cost")
(result
 .withColumn("via", via_udf("path"))
 .select("id", "distance", "via")
 .sort("distance")
 .show(truncate=False))
# // end::execute[]
