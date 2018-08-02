// tag::neo4j-execute[]
CALL algo.scc.stream("Library", "DEPENDS_ON")
YIELD nodeId, partition
MATCH (l) WHERE id(l) = nodeId
RETURN partition, collect(l.id) AS libraries
ORDER BY size(libraries) DESC
// end::neo4j-execute[]

// tag::neo4j-add-circular-dependency[]
MATCH (py4j:Library {id: "py4j"})
MATCH (pyspark:Library {id: "pyspark"})
MERGE (extra:Library {id: "extra"})
MERGE (py4j)-[:DEPENDS_ON]->(extra)
MERGE (extra)-[:DEPENDS_ON]->(pyspark)
// end::neo4j-add-circular-dependency[]


// tag::neo4j-delete-circular-dependency[]
MATCH (extra:Library {id: "extra"})
DETACH DELETE extra
// end::neo4j-delete-circular-dependency[]