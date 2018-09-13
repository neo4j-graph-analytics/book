# tag::imports[]
from neo4j.v1 import GraphDatabase
import pandas as pd
from tabulate import tabulate
# end::imports[]

# tag::matplotlib-imports[]
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
# end::matplotlib-imports[]

# tag::driver[]
driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))
# end::driver[]


# tag::all-hotels[]
query = """\
MATCH (review:Review)-[:REVIEWS]->(business:Business),
      (business)-[:IN_CATEGORY]->(category:Category {name: $category}),
      (business)-[:IN_CITY]->(:City {name: $city})
WITH business,
     count(*) AS reviews,
     avg(review.stars) AS ave,
     collect(review.stars) AS allReviews
WITH business, reviews, ave, apoc.coll.frequencies(allReviews) AS frequencies
RETURN business.name AS business, reviews, ave,
       [f in frequencies where f.item = 1 | f.count][0] AS s1,
       [f in frequencies where f.item = 2 | f.count][0] AS s2,
       [f in frequencies where f.item = 3 | f.count][0] AS s3,
       [f in frequencies where f.item = 4 | f.count][0] AS s4,
       [f in frequencies where f.item = 5 | f.count][0] AS s5
"""

with driver.session() as session:
    params = { "city": "Las Vegas", "category": "Hotels" }
    df = pd.DataFrame([dict(record) for record in session.run(query, params)])
    df = df.round(2)
    df = df[["business", "reviews", "ave", "s1", "s2", "s3", "s4", "s5"]]
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
"""

with driver.session() as session:
    params = { "hotel": "Bellagio Hotel", "goodRating": 4 }
    df = pd.DataFrame([dict(record) for record in session.run(query, params)])
    df = df.round(2)
    df = df[["name", "pageRank", "stars"]]

top_reviews = df.sort_values(by=["pageRank"], ascending=False).head(10)
print(tabulate(top_reviews, headers='keys', tablefmt='psql', showindex=False))
# end::bellagio-bad-rating[]
