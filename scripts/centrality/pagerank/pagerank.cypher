// tag::neo4j-execute[]
CALL algo.pageRank.stream('User', 'FOLLOWS', {iterations:20, dampingFactor:0.85})
YIELD nodeId, score
MATCH (node) WHERE id(node) = nodeId
RETURN node.id AS page, score
ORDER BY score DESC
// end::neo4j-execute[]