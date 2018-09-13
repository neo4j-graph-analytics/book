# tag::imports[]
from neo4j.v1 import GraphDatabase
import pandas as pd
import numpy as np
from tabulate import tabulate

# end::imports[]

# tag::driver[]
driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))
# end::driver[]

# tag::top-ranking-dist[]
query = """
MATCH (u:User)
WHERE exists(u.hotelPageRank)
RETURN count(u.hotelPageRank) AS count,
       avg(u.hotelPageRank) AS ave,
       percentileDisc(u.hotelPageRank, 0.5) AS p50,
       percentileDisc(u.hotelPageRank, 0.75) AS p75,
       percentileDisc(u.hotelPageRank, 0.90) AS p90,
       percentileDisc(u.hotelPageRank, 0.95) AS p95,
       percentileDisc(u.hotelPageRank, 0.99) AS p99,
       percentileDisc(u.hotelPageRank, 0.999) AS p999,
       percentileDisc(u.hotelPageRank, 0.9999) AS p9999,
       percentileDisc(u.hotelPageRank, 0.99999) AS p99999,
       percentileDisc(u.hotelPageRank, 1) AS p100
"""

with driver.session() as session:
    df = pd.DataFrame([dict(record) for record in session.run(query)])
    df = df[["count", "ave", "p50", "p75", "p90", "p95", "p99", "p999", "p9999", "p99999", "p100"]]
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False, floatfmt='.7g'))
# end::top-ranking-dist[]
