# tag::imports[]
from neo4j.v1 import GraphDatabase
import pandas as pd
import numpy as np
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

# Find the top 10 hotels with the most reviews
query = """
MATCH (review:Review)-[:REVIEWS]->(business:Business),
      (business)-[:IN_CATEGORY]->(category:Category {name: $category}),
      (business)-[:IN_CITY]->(:City {name: $city})
RETURN business.name AS business, collect(review.stars) AS allReviews
ORDER BY size(allReviews) DESC
LIMIT 10
"""

fig = plt.figure()
fig.set_size_inches(10.5, 14.5)
fig.subplots_adjust(hspace=0.4, wspace=0.4)

with driver.session() as session:
    params = { "city": "Las Vegas", "category": "Hotels"}
    result = session.run(query, params)
    for index, row in enumerate(result):
        business = row["business"]
        stars = pd.Series(row["allReviews"])

        total = stars.count()
        average_stars = stars.mean().round(2)

        # Calculate the star distribution
        stars_histogram = stars.value_counts().sort_index()
        stars_histogram /= float(stars_histogram.sum())

        # Plot a bar chart showing the distribution of star ratings
        ax = fig.add_subplot(5, 2, index+1)
        stars_histogram.plot(kind="bar", legend=None, color="darkblue",
                             title=f"{business}\nAve: {average_stars}, Total: {total}")

plt.tight_layout()
plt.show()
# end::all-hotels[]


plt.savefig("/tmp/hotels_dist.svg")
plt.close()