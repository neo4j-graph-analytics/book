import networkx as nx
import community

G = nx.Graph()

G.add_nodes_from([
    "six",
    "pandas",
    "numpy",
    "python-dateutil",
    "pytz",
    "pyspark",
    "matplotlib",
    "spacy",
    "py4j",
    "jupyter",
    "jpy-console",
    "nbconvert",
    "ipykernel",
    "jpy-client",
    "jpy-core"
])

G.add_edges_from([
    ("pandas", "numpy"),
    ("pandas", "pytz"),
    ("pandas", "python-dateutil"),
    ("python-dateutil", "six"),
    ("pyspark", "py4j"),
    ("matplotlib", "numpy"),
    ("matplotlib", "python-dateutil"),
    ("matplotlib", "six"),
    ("matplotlib", "pytz"),
    ("spacy", "six"),
    ("spacy", "numpy"),
    ("jupyter", "nbconvert"),
    ("jupyter", "ipykernel"),
    ("jupyter", "jpy-console"),
    ("jpy-console", "jpy-client"),
    ("jpy-console", "ipykernel"),
    ("jpy-client", "jpy-core"),
    ("nbconvert", "jpy-core"),
])

d = community.generate_dendrogram(G)

l0 = community.partition_at_level(d, 0)
print(l0)
print("l0 modularity: " + str(community.modularity(l0, G)))

l1 = community.partition_at_level(d, 1)
print(l1)
print("l1 modularity: " + str(community.modularity(l1, G)))
