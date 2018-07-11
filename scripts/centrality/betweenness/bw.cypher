// tag::neo4j-execute[]
CALL algo.betweenness.stream("User", "FOLLOWS")
YIELD nodeId, centrality
MATCH (user:User) WHERE id(user) = nodeId
RETURN user.id AS user,centrality
ORDER BY centrality DESC
// end::neo4j-execute[]

// tag::neo4j-execute-approx[]
CALL algo.betweenness.sampled.stream("User", "FOLLOWS",
 {strategy:"random", probability:1.0, maxDepth:1})
YIELD nodeId, centrality
MATCH (user) WHERE id(user) = nodeId
RETURN user.id AS user,centrality
ORDER BY centrality DESC
// end::neo4j-execute-approx[]