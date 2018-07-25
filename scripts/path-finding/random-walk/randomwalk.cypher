// tag::neo4j-execute[]
MATCH (source:Place {id: "London"})
CALL algo.randomWalk.stream(id(source), 5, 1)
YIELD nodesIds

UNWIND nodeIds AS nodeId
MATCH (place) WHERE id(place) = nodeId

RETURN place.id AS place
// end::neo4j-execute[]
