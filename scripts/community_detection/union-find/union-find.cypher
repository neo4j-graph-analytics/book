// tag::neo4j-execute[]
CALL algo.unionFind.stream("Library", "DEPENDS_ON", {})
YIELD nodeId,setId
MATCH (l) WHERE id(l) = nodeId
RETURN setId, collect(l.id) AS libraries
ORDER BY size(libraries) DESC
// end::neo4j-execute[]