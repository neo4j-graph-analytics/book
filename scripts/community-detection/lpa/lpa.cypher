// tag::neo4j-execute[]
CALL algo.labelPropagation.stream("Library", "DEPENDS_ON",
  { iterations: 10 })
YIELD nodeId, label
MATCH (n) WHERE id(n) = nodeId
RETURN label, collect(n.id) AS libraries
ORDER BY size(libraries) DESC
// end::neo4j-execute[]

// tag::neo4j-undirected-execute[]
CALL algo.labelPropagation.stream("Library", "DEPENDS_ON",
  { iterations: 10, direction: "BOTH" })
YIELD nodeId, label
MATCH (n) WHERE id(n) = nodeId
RETURN label, collect(n.id) AS libraries
ORDER BY size(libraries) DESC
// end::neo4j-undirected-execute[]