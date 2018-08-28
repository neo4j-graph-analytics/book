// tag::count[]
MATCH (category:Category {name: "Hotels"})
RETURN size((category)<-[:IN_CATEGORY]-()) AS count
// end::count[]

// tag::reviews[]
MATCH (:Review)-[:REVIEWS]->(:Business)-[:IN_CATEGORY]->(category:Category {name: "Hotels"})
RETURN count(*) AS count
// end::reviews[]

// tag::top-rated[]
MATCH (review:Review)-[:REVIEWS]->(business:Business),
      (business)-[:IN_CATEGORY]->(:Category {name:"Hotels"}),
      (business)-[:IN_CITY]->(:City {name: "Las Vegas"})
WITH business, count(*) AS reviews, avg(review.stars) AS averageRating
ORDER BY reviews DESC
LIMIT 10
RETURN business.name AS business,
       reviews,
       averageRating
// end::top-rated[]

// tag::best-reviewers[]
MATCH (u:User)-[:WROTE]->()-[:REVIEWS]->(business),
      (business)-[:IN_CATEGORY]->(category:Category {name: "Hotels"}),
      (business)-[:IN_CITY]->(:City {name: "Las Vegas"})
WITH u, count(*) AS reviews
WHERE reviews > 5
WITH collect(u) AS hotelReviewers

CALL algo.pageRank(
  "MATCH (u:User) RETURN id(u) AS id",
  "MATCH (u1:User)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->(category:Category)
   WHERE category.name = 'Hotels'
   MATCH (u1)-[:FRIENDS]->(u2)
   WHERE id(u1) < id(u2)
   RETURN id(u1) AS source, id(u2) AS target",
  {graph: "cypher", writeProperty: "hotelPageRank",
   direction: "BOTH", sourceNodes: hotelReviewers}
)
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, writeProperty
RETURN nodes, iterations, loadMillis, computeMillis, writeMillis, writeProperty
// end::best-reviewers[]

// tag::best-reviewers-query[]
MATCH (u:User)
WITH u
ORDER BY u.hotelPageRank DESC
MATCH (u)-[:WROTE]->()-[:REVIEWS]->()-[:IN_CATEGORY]->(category:Category {name: "Hotels"})
RETURN u.name AS name,
       u.hotelPageRank AS pageRank,
       count(*) AS hotelReviews,
       size((u)-[:WROTE]->()) AS totalReviews,
       size((u)-[:FRIENDS]-()) AS friends
ORDER BY u.hotelPageRank DESC
// end::best-reviewers-query[]

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
