
// tag::stream[]
CALL algo.louvain.stream("Library", "DEPENDS_ON")
YIELD nodeId, communities
RETURN algo.getNodeById(nodeId).id AS libraries, communities
// end::stream[]


// tag::write[]
CALL algo.louvain("Library", "DEPENDS_ON")
// end::write[]


// tag::read-layer-1[]
MATCH (l:Library)
RETURN l.communities[0] AS community, collect(l.id) AS libraries
ORDER BY size(libraries) DESC
// end::read-layer-1[]


// tag::read-layer-2[]
MATCH (l:Library)
RETURN l.communities[-1] AS community, collect(l.id) AS libraries
ORDER BY size(libraries) DESC
// end::read-layer-2[]
