// tag::neo4j-execute-unweighted[]
CALL algo.allShortestPaths.stream(null, {nodeQuery:"Place", defaultValue:1.0})
YIELD sourceNodeId, targetNodeId, distance

WITH sourceNodeId, targetNodeId, distance WHERE algo.isFinite(distance) = true
MATCH (source) WHERE id(source) = sourceNodeId
MATCH (target) WHERE id(target) = targetNodeId

WITH source.id AS source, target.id AS target, distance
ORDER BY source, target
RETURN source, apoc.map.fromPairs(collect([target, distance])) AS others
ORDER BY source
// end::neo4j-execute-unweighted[]

// tag::neo4j-execute-weighted[]
CALL algo.allShortestPaths.stream("distance", {nodeQuery:"Place", defaultValue:1.0})
YIELD sourceNodeId, targetNodeId, distance

WITH sourceNodeId, targetNodeId, distance WHERE algo.isFinite(distance) = true
MATCH (source) WHERE id(source) = sourceNodeId
MATCH (target) WHERE id(target) = targetNodeId

WITH source.id AS source, target.id AS target, distance
ORDER BY source, target
RETURN source, apoc.map.fromPairs(collect([target, distance])) AS others
ORDER BY source
// end::neo4j-execute-weighted[]

MATCH (n:Place {id:"London"})
CALL algo.shortestPath.deltaStepping.stream(n, "distance", 3.0)
YIELD nodeId, distance
MATCH (destination) WHERE id(destination) = nodeId
RETURN destination.id AS destination, distance