// tag::neo4j-execute[]
CALL algo.closeness.stream("User", "FOLLOWS")
YIELD nodeId, centrality
MATCH (user) WHERE id(user) = nodeId
RETURN user.id AS user, centrality
ORDER BY centrality DESC
// end::neo4j-execute[]

// tag::neo4j-write-execute[]
CALL algo.closeness("User", "FOLLOWS")
// end::neo4j-write-execute[]

// tag::neo4j-execute-wasserman-faust[]
CALL algo.closeness.stream("User", "FOLLOWS", {improved: true})
YIELD nodeId, centrality
MATCH (user) WHERE id(user) = nodeId
RETURN user.id AS user, centrality
ORDER BY centrality DESC
// end::neo4j-execute-wasserman-faust[]

// tag::neo4j-execute-harmonic[]
CALL algo.closeness.harmonic.stream("User", "FOLLOWS")
YIELD nodeId, centrality
MATCH (user) WHERE id(user) = nodeId
RETURN user.id AS user, centrality
ORDER BY centrality DESC
// end::neo4j-execute-harmonic[]
