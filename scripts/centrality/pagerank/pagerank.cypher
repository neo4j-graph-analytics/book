// tag::neo4j-execute[]
CALL algo.pageRank.stream('User', 'FOLLOWS', {iterations:20, dampingFactor:0.85})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId).id AS page, score
ORDER BY score DESC
// end::neo4j-execute[]

// tag::neo4j-write-execute[]
CALL algo.pageRank('User', 'FOLLOWS', {iterations:20, dampingFactor:0.85})
// end::neo4j-write-execute[]
