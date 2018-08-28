# tag::imports[]
from neo4j.v1 import GraphDatabase
import pandas as pd
from tabulate import tabulate
# end::imports[]

# tag::driver[]
driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", "neo"))
# end::driver[]


# tag::toronto-restaurants[]
query = """\
MATCH (review:Review)-[:REVIEWS]->(business:Business),
      (business)-[:IN_CATEGORY]->(category:Category),
      (business)-[:IN_CITY]->(:City {name: $city})
WHERE category.name in $categories
WITH business, count(*) AS reviews, avg(review.stars) AS averageRating
RETURN business.name AS business, reviews, averageRating
"""
with driver.session() as session:
    params = { "city": "Toronto", "categories": ["Food", "Restaurants"] }
    df = pd.DataFrame([dict(record) for record in session.run(query, params)])
# end::toronto-restaurants[]

# tag::toronto-restaurants-top-rated[]
top_restaurants = df.sort_values(by=["reviews"], ascending=False).head(10)
print(tabulate(top_restaurants, headers='keys', tablefmt='psql', showindex=False))
# tag::toronto-restaurants-top-rated[]