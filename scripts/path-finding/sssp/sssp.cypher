// tag::neo4j-execute[]
MATCH (n:Place {id:"London"})
CALL algo.shortestPath.deltaStepping.stream(n, "distance", 3.0)
YIELD nodeId, distance
MATCH (destination) WHERE id(destination) = nodeId
RETURN destination.id AS destination, distance
// end::neo4j-execute[]