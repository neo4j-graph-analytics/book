// tag::neo4j-import-nodes[]
WITH "https://github.com/neo4j-graph-analytics/book/raw/master/data/social-nodes.csv"
AS uri
LOAD CSV WITH HEADERS FROM uri AS row
MERGE (:User {id: row.id})
// end::neo4j-import-nodes[]

// tag::neo4j-import-relationships[]
WITH "https://github.com/neo4j-graph-analytics/book/raw/master/data/social-relationships.csv"
AS uri
LOAD CSV WITH HEADERS FROM uri AS row
MATCH (source:User {id: row.src})
MATCH (destination:User {id: row.dst})
MERGE (source)-[:FOLLOWS]->(destination)
// end::neo4j-import-relationships[]
