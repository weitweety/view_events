from pyspark.sql import SparkSession
from src.analyser import *

spark = SparkSession.builder.getOrCreate()

df = spark.table("dev.default.view_events")

agg_reaction_df = aggregate_reactions(df)
agg_bin_duration = aggregate_duration(df)
agg_visit_df = aggregate_visits(df)

agg_reaction_df.write.saveAsTable("dev.default.agg_reaction_df", mode="overwrite")
agg_bin_duration.write.saveAsTable("dev.default.agg_bin_duration", mode="overwrite")
agg_visit_df.write.saveAsTable("dev.default.agg_visit_df", mode="overwrite")
