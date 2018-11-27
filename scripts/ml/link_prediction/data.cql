// tag::import-citation[]
USING PERIODIC COMMIT 1000
LOAD CSV FROM "file:///training_set.txt" AS row FIELDTERMINATOR " "
WITH row WHERE row[2] = "1"
MERGE (n1:Paper {id: row[0]})
MERGE (n2:Paper {id: row[1]})
MERGE (n1)-[:CITE]-(n2);
// end::import-citation[]

// tag::import-no-citation[]
USING PERIODIC COMMIT 1000
LOAD CSV FROM "file:///training_set.txt" AS row FIELDTERMINATOR " "
WITH row WHERE row[2] = "0"
MERGE (n1:Paper {id: row[0]})
MERGE (n2:Paper {id: row[1]})
MERGE (n1)-[:NO_CITATION]-(n2);
// end::import-no-citation[]


// tag::import-authors[]
LOAD CSV FROM "file:///node_information.csv" AS row
MATCH (p:Paper {id: row[0]})
WITH p, [author in split(row[3], ",") | trim(author)] AS authors
UNWIND authors AS author
MERGE (a:Author {name: author})
MERGE (p)-[:AUTHOR]->(a);
// end::import-authors[]

// tag::jaccard[]
CALL apoc.periodic.iterate(
 'MATCH (p1:Paper)-[link:CITE|:NO_CITATION]->(p2) RETURN p1, p2, link',
 'SET link.jaccard = algo.similarity.jaccard([(p1)-[:AUTHOR]-> (a) | id(a)], [(p2)-[:AUTHOR]-> (a) | id(a)])',
 {});
// end::jaccard[]

// tag::lpa[]
call algo.labelPropagation("Paper", "CITE", "OUTGOING");
// end::lpa[]


// tag::pagerank[]
call algo.pageRank("Paper", "CITE");
// end::pagerank[]

// tag::triangle[]
CALL algo.triangleCount('Paper', 'CITE',
  {write:true, writeProperty:'triangles',clusteringCoefficientProperty:'coefficient'});
// end::triangle[]
