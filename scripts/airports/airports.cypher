
// tag::page-rank[]
CALL algo.pageRank.stream('Airport','CONNECTED_TO')
YIELD nodeId,score
MATCH (n) WHERE id(n) = nodeId
RETURN n.name AS airport, score
ORDER BY score DESC
LIMIT 10
// end::page-rank[]

// tag::betweenness[]
CALL algo.betweenness.stream('Airport','CONNECTED_TO')
YIELD nodeId,centrality
MATCH (n) WHERE id(n)=nodeId
RETURN n.name as airport, centrality
ORDER BY centrality DESC
LIMIT 10
// end::betweenness[]


// tag::scc[]
MATCH ()-[r:CONNECTED_TO]->()
WITH r.airline as airline_carrier,count(*) as number_of_flights
ORDER BY number_of_flights DESC LIMIT 10
CALL apoc.cypher.run("
  CALL algo.scc.stream(
    'MATCH (a:Airport) RETURN id(a) as id',
    'MATCH (a1:Airport)-[c:CONNECTED_TO]->(a2:Airport)
     WHERE c.airline = \"' + airline_carrier + '\"
     RETURN id(a1) as source,id(a2) as target',
    {graph:'cypher'})
  YIELD nodeId, partition
  RETURN partition, count(*) as size
  ORDER BY size DESC
  LIMIT 1",
  {airline_carrier:airline_carrier})
YIELD value
RETURN airline_carrier,number_of_flights, value.size as biggest_scc_size
// end::scc[]

// tag::lpa[]
CALL algo.labelPropagation.stream(
  "MATCH (a:Airport) RETURN id(a) as id",
  "MATCH (a1:Airport)-[r:CONNECTED_TO]->(a2:Airport)
   WHERE r.airline = "OO"
   WITH a1, a2, count(*) as weight
   RETURN id(a1) as source, id(a2) as target, weight",
  {graph:'cypher'})
YIELD nodeId,label
RETURN label, count(*) as size
ORDER BY size DESC
LIMIT 10
// end::lpa[]

