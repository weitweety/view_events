from pyspark.sql.functions import col, when

def aggregate_reactions(df):
    total = df.count()
    agg_reaction_df = (
        df
        .groupBy("reaction")
        .count()
        .withColumnRenamed("count", "reaction_count")
        .withColumn("reaction_ratio", col("reaction_count") / total)
    )
    return agg_reaction_df

def aggregate_duration(df):
    total = df.count()
    bin_duration = df.withColumn("bin_duration", when(col("view_duration") <= 5, "0-5")
                                .when((col("view_duration") > 5) & (col("view_duration") <= 10), "5-10")
                                .when((col("view_duration") > 10) & (col("view_duration") <= 20), "10-20")
                                .otherwise("Invalid view time"))
    agg_bin_duration = (
        bin_duration
        .groupBy("bin_duration")
        .count()
        .withColumnRenamed("count", "bin_duration_count")
        .withColumn("bin_duration_ratio", col("bin_duration_count") / total)
    )
    return agg_bin_duration

def aggregate_visits(df):
    total = df.count()
    agg_visit_df = (
        df
        .groupBy("visit_website")
        .count()
        .withColumnRenamed("count", "category_count")
        .withColumn("category_ratio", col("category_count") / total)
    )
    return agg_visit_df
