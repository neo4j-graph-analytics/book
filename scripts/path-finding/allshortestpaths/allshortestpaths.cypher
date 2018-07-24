// tag::neo4j-execute-unweighted[]
CALL algo.allShortestPaths.stream(null, {nodeQuery:"Place", defaultValue:1.0})
YIELD sourceNodeId, targetNodeId, distance

WITH sourceNodeId, targetNodeId, distance WHERE algo.isFinite(distance) = true
MATCH (source) WHERE id(source) = sourceNodeId
MATCH (target) WHERE id(target) = targetNodeId

RETURN source.id AS source, target.id AS target, distance
ORDER BY distance DESC
LIMIT 10
// end::neo4j-execute-unweighted[]

// tag::neo4j-execute-weighted[]
CALL algo.allShortestPaths.stream("distance", {nodeQuery:"Place", defaultValue:1.0})
YIELD sourceNodeId, targetNodeId, distance

WITH sourceNodeId, targetNodeId, distance WHERE algo.isFinite(distance) = true
MATCH (source) WHERE id(source) = sourceNodeId
MATCH (target) WHERE id(target) = targetNodeId

RETURN source.id AS source, target.id AS target, distance
ORDER BY distance DESC
LIMIT 10
// end::neo4j-execute-weighted[]
