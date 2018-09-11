# tag::imports[]
from neo4j.v1 import GraphDatabase
import pandas as pd
from tabulate import tabulate
# end::imports[]

# tag::driver[]
driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))
# end::driver[]


# tag::all-hotels[]
query = """\
MATCH (review:Review)-[:REVIEWS]->(business:Business),
      (business)-[:IN_CATEGORY]->(category:Category {name: $category}),
      (business)-[:IN_CITY]->(:City {name: $city})
WITH business, count(*) AS reviews, avg(review.stars) AS averageRating
RETURN business.name AS business, reviews, averageRating
"""

with driver.session() as session:
    params = { "city": "Las Vegas", "category": "Hotels" }
    df = pd.DataFrame([dict(record) for record in session.run(query, params)])
    df = df.round(2)
    df = df[["business", "reviews", "averageRating"]]
# end::all-hotels[]

# tag::top-rated[]
top_hotels = df.sort_values(by=["reviews"], ascending=False).head(10)
print(tabulate(top_hotels, headers='keys', tablefmt='psql', showindex=False))
# end::top-rated[]


# tag::bellagio[]
query = """\
MATCH (b:Business {name: $hotel})
MATCH (b)<-[:REVIEWS]-(review)<-[:WROTE]-(user)
WHERE exists(user.hotelPageRank)
RETURN user.name AS name,
       user.hotelPageRank AS pageRank,
       review.stars AS stars
"""

with driver.session() as session:
    params = { "hotel": "Bellagio Hotel" }
    df = pd.DataFrame([dict(record) for record in session.run(query, params)])
    df = df.round(2)
    df = df[["name", "pageRank", "stars"]]

top_reviews = df.sort_values(by=["pageRank"], ascending=False).head(10)
print(tabulate(top_reviews, headers='keys', tablefmt='psql', showindex=False))
# end::bellagio[]

# tag::bellagio-bad-rating[]
query = """\
MATCH (b:Business {name: $hotel})
MATCH (b)<-[:REVIEWS]-(review)<-[:WROTE]-(user)
WHERE exists(user.hotelPageRank) AND review.stars < $goodRating
RETURN user.name AS name,
       user.hotelPageRank AS pageRank,
       review.stars AS stars
ORDER BY user.hotelPageRank DESC
"""

with driver.session() as session:
    params = { "hotel": "Bellagio Hotel", "goodRating": 4 }
    df = pd.DataFrame([dict(record) for record in session.run(query, params)])
    df = df.round(2)
    df = df[["name", "pageRank", "stars"]]

top_reviews = df.sort_values(by=["pageRank"], ascending=False).head(10)
print(tabulate(top_reviews, headers='keys', tablefmt='psql', showindex=False))
# end::bellagio-bad-rating[]
