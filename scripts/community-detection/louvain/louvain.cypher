
// tag::neo4j-execute[]
CALL algo.louvain.stream("Library", "DEPENDS_ON", {})
YIELD nodeId, community
MATCH (l:Library) WHERE id(l) = nodeId
RETURN community,collect(l.id) AS libraries
ORDER BY community
// end::neo4j-execute[]