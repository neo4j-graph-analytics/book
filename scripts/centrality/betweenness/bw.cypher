// tag::neo4j-execute[]
CALL algo.betweenness.stream("User", "FOLLOWS")
YIELD nodeId, centrality
RETURN algo.getNodeById(nodeId).id  AS user, centrality
ORDER BY centrality DESC
// end::neo4j-execute[]

// tag::neo4j-write-execute[]
CALL algo.betweenness('User', 'FOLLOWS')
// end::neo4j-write-execute[]

// tag::neo4j-execute-approx[]
CALL algo.betweenness.sampled.stream("User", "FOLLOWS", {strategy:"degree"})
YIELD nodeId, centrality
RETURN algo.getNodeById(nodeId).id AS user, centrality
ORDER BY centrality DESC
// end::neo4j-execute-approx[]

// tag::neo4j-add-local-bridge-execute[]

WITH ["James", "Michael", "Alice", "Doug", "Amy"] AS existingUsers

MATCH (existing:User) WHERE existing.id IN existingUsers
MERGE (newUser:User {id: "Jason"})

MERGE (newUser)<-[:FOLLOWS]-(existing)
MERGE (newUser)-[:FOLLOWS]->(existing)

// end::neo4j-add-local-bridge-execute[]

// tag::neo4j-remove-local-bridge-execute[]

MATCH (user:User {id: "Jason"})
DETACH DELETE user

// end::neo4j-remove-local-bridge-execute[]
