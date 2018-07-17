// tag::neo4j-execute[]
MATCH (u:User)
RETURN u.id AS name,
       size((u)-[:FOLLOWS]->()) AS follows,
       size((u)<-[:FOLLOWS]-()) AS followers
// end::neo4j-execute[]

// tag::neo4j-write-execute[]
MATCH (u:User)
set u.followers = size((u)<-[:FOLLOWS]-())
// end::neo4j-write-execute[]
