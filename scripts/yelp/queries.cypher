// tag::restaurants[]
MATCH (category)<-[:IN_CATEGORY]-(business:Business)
WHERE category.name IN ["Restaurants", "Food"]
RETURN count(*) AS count
// end::restaurants[]

// tag::restaurants-reviews[]
MATCH (:Review)-[:REVIEWS]->(:Business)-[:IN_CATEGORY]->(category:Category)
WHERE category.name IN ["Restaurants", "Food"]
RETURN count(*) AS count
// end::restaurants-reviews[]
