// tag::neo4j-execute[]
MATCH (source:Place {id: "Amsterdam"})
CALL algo.randomWalk.stream(source, 10, 1)
YIELD nodesIds
RETURN nodeIds
// end::neo4j-execute[]
