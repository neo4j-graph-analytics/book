
// tag::neo4j-execute[]
MATCH (start:Place {id:"London"}),
      (end:Place {id:"Amsterdam"})
CALL algo.kShortestPaths(start, end, 3, 'distance' ,{})
YIELD resultCount
RETURN resultCount
// end::neo4j-execute[]

// tag::neo4j-path0-execute[]
MATCH p=(:Place {id: "London"})-[r:PATH_0*]->(:Place {id: "Amsterdam"})
UNWIND relationships(p) AS pair
return startNode(pair).id, endNode(pair).id, pair.weight AS distance
// end::neo4j--path0-execute[]

// tag::neo4j-path1-execute[]
MATCH p=(:Place {id: "London"})-[r:PATH_1*]->(:Place {id: "Amsterdam"})
UNWIND relationships(p) AS pair
return startNode(pair).id, endNode(pair).id, pair.weight AS distance
// end::neo4j--path1-execute[]