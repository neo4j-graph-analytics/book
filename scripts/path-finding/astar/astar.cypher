// tag::neo4j-execute[]
MATCH (source:Place {id: "Den Haag"}),
      (destination:Place {id: "London"})
CALL algo.shortestPath.astar.stream(source, destination, "distance")
YIELD nodeId, cost
MATCH (p) WHERE id(p) = nodeId
RETURN p.id AS place, cost
// end::neo4j-execute[]
