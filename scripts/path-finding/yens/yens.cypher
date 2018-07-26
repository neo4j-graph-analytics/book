
// tag::neo4j-execute[]
MATCH (start:Place {id:"London"}),
      (end:Place {id:"Amsterdam"})
CALL algo.kShortestPaths.stream(start, end, 3, 'distance')
YIELD index, nodeIds, path, costs
RETURN index,
       [nodeId in nodeIds | algo.getNodeById(nodeId).id][1..-1] AS via,
       reduce(acc=0.0, cost in costs | acc + cost) AS totalCosta
// end::neo4j-execute[]
