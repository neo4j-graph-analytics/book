
// tag::neo4j-execute[]
MATCH (start:Place {id:"London"}),
      (end:Place {id:"Amsterdam"})
CALL algo.kShortestPaths.stream(end, start, 3, 'distance' ,{path:true})
YIELD index, nodeIds, path, costs
RETURN index,
       [nodeId in nodeIds | algo.getNodeById(nodeId).id][1..-1] AS via,
       reduce(acc=0.0, cost in costs | acc + cost) AS totalCost
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
