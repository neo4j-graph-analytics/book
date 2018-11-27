# tag::label[]
data.describe().select("summary", "label").show()
# end::label[]

# tag::label-agg[]
data.groupBy("label").agg(F.count("label").alias("count")).show()
# end::label-agg[]

# tag::common-authors[]
data.stat.corr("commonAuthors", "label")
# end::common-authors[]
