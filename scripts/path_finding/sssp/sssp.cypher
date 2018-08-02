// tag::neo4j-execute[]
MATCH (n:Place {id:"London"})
CALL algo.shortestPath.deltaStepping.stream(n, "distance", 1.0)
YIELD nodeId, distance WHERE algo.isFinite(distance)
MATCH (destination) WHERE id(destination) = nodeId
RETURN destination.id AS destination, distance
ORDER BY distance
// end::neo4j-execute[]
