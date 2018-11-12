# tag::imports[]
from py2neo import Graph
import pandas as pd

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
# end::imports[]

# tag::py2neo[]
graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo"))
# end::py2neo[]

# tag::eda[]

# end::eda[]

# tag::feature-matrix[]
query = """
MATCH (cat:Category)<-[:IN_CATEGORY]-(business:Business)-[:IN_CITY]->(city:City)
WHERE cat.name = $category AND city.name = $city
MATCH (business:Business)<-[:REVIEWS]-(review:Review)<-[:WROTE]-(user:User)
RETURN user.id AS userId,
       size((user)-[:WROTE]->()) AS numberOfReviews,
       size((business)-[:HAS_PHOTO]->()) AS numberOfPhotos,
       size((business)<-[:WROTE_TIP]-()) AS numberOfTips,
       apoc.coll.avg([(user)-[:WROTE]->(r) WHERE not((r)-[:REVIEWS]->(business)) | r.stars]) AS aveMyStars,
       apoc.coll.avg([(business)<-[:REVIEWS]-(r) WHERE not((r)<-[:WROTE]->(user)) | r.stars]) AS aveBusinessStars,
       CASE WHEN review.stars > 3 THEN "true" ELSE "false" END as stars
"""

df = graph.run(query, {"city": "Las Vegas", "category": "Restaurants"}).to_data_frame()
data = spark.createDataFrame(df)
data.select("userId", "numberOfReviews", "numberOfPhotos", "numberOfTips", "stars").show(truncate=False)
# end::feature-matrix[]

# tag::feature-matrix-columns[]
columns = ["numberOfReviews", "numberOfPhotos", "numberOfTips", "aveMyStars"]
# end::feature-matrix-columns[]

# tag::feature-matrix-graph[]
query = """
MATCH (:Category {name: $category})<-[:IN_CATEGORY]-(business:Business)-[:IN_CITY]->(:City {name: $city})
MATCH (business:Business)<-[:REVIEWS]-(review:Review)<-[:WROTE]-(user:User)
WITH business, user,
     size((user)-[:FRIENDS]->()) AS userFriends,
     review.stars AS stars,
     review
LIMIT 1000
OPTIONAL MATCH (business)<-[:REVIEWS]-(otherRev)<-[:WROTE]-(other:User)-[:FRIENDS]->(user) WHERE otherRev.date < review.date
WITH  user, business, userFriends, avg(otherRev.stars) AS aveOtherStars, stars
OPTIONAL MATCH (user)-[:WROTE]->(review) WHERE not((review)-[:REVIEWS]->(business))
RETURN user.id AS userId,
       userFriends,
       aveOtherStars,
       avg(review.stars) AS aveMyStars,
       CASE WHEN stars > 3 THEN "true" ELSE "false" END as stars
"""

df = graph.run(query, {"city": "Las Vegas", "category": "Restaurants"}).to_data_frame()
data = spark.createDataFrame(df)
data.select("userId", "userFriends", "aveMyStars", "aveOtherStars", "stars").show(truncate=False)
# end::feature-matrix-graph[]

# tag::feature-matrix-graph-columns[]
columns = ["userFriends", "aveMyStars", "aveOtherStars"]
# end::feature-matrix-graph-columns[]

# tag::prep[]
def create_pipeline(columns):
    assembler = VectorAssembler(inputCols=columns, outputCol="features")
    labelIndexer = StringIndexer(inputCol="stars", outputCol="label")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=30, maxDepth=10)
    return Pipeline(stages=[assembler, labelIndexer, rf])

pipeline = create_pipeline(columns)
# end::prep[]

# tag::train-test-split[]
(training_data, test_data) = data.randomSplit([0.7, 0.3])
# end::train-test-split[]

# tag::sampling-function[]
rand_gen = lambda x: randint(0, higherBound) if x == "true" else -1
udf_rand_gen = udf(rand_gen, IntegerType())

def down_sample(data, ratio_adjust=1.2):
    counts = (data.select('stars').groupBy('stars').count()
        .orderBy(F.column("count"), ascending=False)
        .collect())
    higherBound = counts[0][1]
    threshold_to_filter = int(ratio_adjust * float(counts[1][1]) / counts[0][1] * higherBound)
    data = data.withColumn("randIndex", udf_rand_gen("stars"))
    sampled_training_data = data.filter(data['randIndex'] < threshold_to_filter)
    return sampled_training_data.drop('randIndex')
# end::sampling-function[]

# tag::sampling[]
sampled_training_data = down_sample(training_data)
# end::sampling[]

# tag::train[]
model = pipeline.fit(sampled_training_data)
# end::train[]

# tag::predict[]
predictions = model.transform(test_data)
# end::predict[]

predictions.select("label", "prediction", "features").show(10,truncate=False)
predictions.groupBy("prediction").agg(F.count("prediction")).show()

# tag::evaluate-function[]
def evaluate(predictions):
    evaluator = BinaryClassificationEvaluator()
    accuracy = evaluator.evaluate(predictions)
    tp = predictions[(predictions.label == 1) & (predictions.prediction == 1)].count()
    tn = predictions[(predictions.label == 0) & (predictions.prediction == 0)].count()
    fp = predictions[(predictions.label == 0) & (predictions.prediction == 1)].count()
    fn = predictions[(predictions.label == 1) & (predictions.prediction == 0)].count()
    recall = float(tp)/(tp + fn)
    precision = float(tp) / (tp + fp)
    return pd.DataFrame({
        "Measure": ["Accuracy", "Precision", "Recall"],
        "Score": [accuracy, precision, recall]
    })
# end::evaluate-function[]

# tag::evaluate[]
evaluated_df = evaluate(predictions)
print(evaluated_df)
# end::evaluate[]

# tag::model-exploration[]
rfModel = model.stages[2]
rfModel.featureImportances
# end::model-exploration[]
