
// tag::neo4j-execute[]
CALL algo.louvain.stream("Library", "DEPENDS_ON", {})
YIELD nodeId, community
RETURN community,
       collect(algo.getNodeById(nodeId).id) AS libraries
ORDER BY community
// end::neo4j-execute[]