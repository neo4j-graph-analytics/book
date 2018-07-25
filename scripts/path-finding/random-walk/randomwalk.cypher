// tag::neo4j-execute[]
MATCH (source:Place {id: "London"})
CALL algo.randomWalk.stream(id(source), 5, 1)
YIELD nodesIds
RETURN nodeIds
// end::neo4j-execute[]
