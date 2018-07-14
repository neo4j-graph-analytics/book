// tag::neo4j-execute[]
CALL algo.triangleCount.stream('Library', 'DEPENDS_ON')
YIELD nodeId, triangles, coefficient

MATCH (l) WHERE id(l) = nodeId

RETURN l.id AS library, coefficient
ORDER BY coefficient DESC
// end::neo4j-execute[]


// tag::neo4j-triangle-stream-execute[]
CALL algo.triangle.stream("Library","DEPENDS_ON")
YIELD nodeA,nodeB,nodeC
MATCH (a) WHERE id(a) = nodeA
MATCH (b) WHERE id(b) = nodeB
MATCH (c) WHERE id(c) = nodeC
RETURN a.id AS nodeA, b.id AS nodeB, c.id AS node
// end::neo4j-triangle-stream-execute[]