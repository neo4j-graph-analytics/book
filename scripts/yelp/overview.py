# tag::imports[]
from neo4j.v1 import GraphDatabase
import pandas as pd
from tabulate import tabulate
# end::imports[]

# tag::matplotlib-imports[]
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
# end::matplotlib-imports[]

# tag::driver[]
driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))
# end::driver[]

# tag::node-cardinalities[]
result = {"label": [], "count": []}
with driver.session() as session:
    labels = [row["label"] for row in session.run("CALL db.labels()")]
    for label in labels:
        query = f"MATCH (:`{label}`) RETURN count(*) as count"
        count = session.run(query).single()["count"]
        result["label"].append(label)
        result["count"].append(count)

df = pd.DataFrame(data=result)
print(tabulate(df.sort_values("count"), headers='keys', tablefmt='psql', showindex=False))
# end::node-cardinalities[]

# tag::node-cardinalities-plot[]
plt.style.use('fivethirtyeight')

ax = df.plot(kind='bar', x='label', y='count', legend=None)

ax.xaxis.set_label_text("")
plt.yscale("log")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
# end::node-cardinalities-plot[]
plt.savefig("/tmp/labels.svg")
plt.close()

# tag::rel-cardinalities[]
result = {"relType": [], "count": []}
with driver.session() as session:
    rel_types = [row["relationshipType"] for row in session.run("CALL db.relationshipTypes()")]
    for rel_type in rel_types:
        query = f"MATCH ()-[:`{rel_type}`]->() RETURN count(*) as count"
        count = session.run(query).single()["count"]
        result["relType"].append(rel_type)
        result["count"].append(count)

df = pd.DataFrame(data=result)
print(tabulate(df.sort_values("count"), headers='keys', tablefmt='psql', showindex=False))
# end::rel-cardinalities[]

# tag::rel-cardinalities-plot[]
plt.style.use('fivethirtyeight')

ax = df.plot(kind='bar', x='relType', y='count', legend=None)

ax.xaxis.set_label_text("")
plt.yscale("log")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
# end::rel-cardinalities-plot[]
plt.savefig("/tmp/rels.svg")
plt.close()
