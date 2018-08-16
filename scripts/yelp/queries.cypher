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

// tag::toronto-restaurants-best-reviewers[]
MATCH (u:User)-[:WROTE]->()-[:REVIEWS]->(business)-[:IN_CATEGORY]->(category:Category),
      (business)-[:IN_CITY]->(:City {name: "Toronto"})
WHERE category.name IN ["Restaurants", "Food"]
WITH u, count(*) AS reviews
WHERE reviews > 5
WITH collect(u) AS restaurantReviewers

CALL algo.pageRank(
  "MATCH (u:User) RETURN id(u) AS id",
  "MATCH (u1:User)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->(category:Category)
   WHERE category.name IN ['Restaurants', 'Food']
   MATCH (u1)-[:FRIENDS]->(u2)
   WHERE id(u1) < id(u2)
   RETURN id(u1) AS source, id(u2) AS target",
  {graph: "cypher", writeProperty: "restaurantPageRank",
   direction: "BOTH", sourceNodes: restaurantReviewers}
)
YIELD nodes, iterations, loadMillis, computeMillis,
      writeMillis, dampingFactor, write, writeProperty
RETURN *
// end::toronto-restaurants-best-reviewers[]

// tag::toronto-restaurants-best-reviewers-query[]
MATCH (u:User)
WITH u
ORDER BY u.restaurantPageRank DESC
LIMIT 10
MATCH (u)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->(category:Category)
WHERE category.name IN ["Restaurants", "Food"]
RETURN u.name AS name,
       u.restaurantPageRank AS pageRank,
       count(*) AS restaurantReviews,
       size((u)-[:WROTE]->()) AS totalReviews,
       size((u)-[:FRIENDS]-()) AS friends
ORDER BY u.restaurantPageRank DESC
// end::toronto-restaurants-best-reviewers-query[]

// tag::toronto-restaurants-pai-northern-thai-kitchen[]
MATCH (b:Business {name: "Pai Northern Thai Kitchen"})
MATCH (b)<-[:REVIEWS]-(review)<-[:WROTE]-(user)
RETURN user.name AS name,
       user.restaurantPageRank AS pageRank,
       review.stars AS stars,
       review.date AS date
ORDER BY user.restaurantPageRank DESC
LIMIT 5
// end::toronto-restaurants-pai-northern-thai-kitchen[]

// tag::category-hierarchies[]
CALL algo.louvain.stream(
  "MATCH (c:Category) RETURN id(c) AS id",
  "MATCH (c1:Category)<-[:IN_CATEGORY]-()-[:IN_CATEGORY]->(c2:Category)
   WHERE id(c1) < id(c2)
   RETURN id(c1) AS source, id(c2) AS target, count(*) AS weight",
  {graph: "cypher"}
)
YIELD nodeId, communities
RETURN algo.getNodeById(nodeId).name, communities
// end::category-hierarchies[]


// tag::user-clusters[]
CALL algo.labelPropagation.stream(
  "MATCH (u:User) RETURN id(u) AS id",
  "MATCH (u1)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->(c1:Category),
         (c1)<-[:IN_CATEGORY]-()<-[:REVIEWS]-()<-[:WROTE]-(u2:User)
   WHERE id(u1) < id(u2)
   RETURN id(u1) AS source, id(u2) AS target, count(*) AS weight",
   {graph: "cypher"})
YIELD nodeId, label
RETURN nodeId, label
// end::user-clusters[]
