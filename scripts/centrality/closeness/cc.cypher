// tag::neo4j-execute[]
CALL algo.closeness.stream("User", "FOLLOWS")
YIELD nodeId, centrality
MATCH (n) WHERE id(n) = nodeId
RETURN n.id AS user, centrality
ORDER BY centrality DESC
// end::neo4j-execute[]

// tag::neo4j-execute-wasserman-faust[]
CALL algo.closeness.stream("User", "FOLLOWS", {improved: true})
YIELD nodeId, centrality
MATCH (n) WHERE id(n) = nodeId
RETURN n.id AS user, centrality
ORDER BY centrality DESC
// end::neo4j-execute-wasserman-faust[]

// tag::neo4j-execute-harmonic[]
CALL algo.closeness.harmonic.stream("User", "FOLLOWS")
YIELD nodeId, centrality
MATCH (n) WHERE id(n) = nodeId
RETURN n.id AS user, centrality
ORDER BY centrality DESC
// end::neo4j-execute-harmonic[]