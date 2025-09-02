import sys
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import SparkSession

def ingest_stream(input_path, table_name, checkpoint_path):
  # e.g. dbfs:/Volumes/dev/default/events_stream/
  # e.g. dev.default.view_events
  # e.g. dbfs:/Volumes/dev/default/autoloader_checkpoints/view_events
  spark = SparkSession.builder.getOrCreate()
  # Clean up old table & checkpoints
  # spark.sql(f"DROP TABLE IF EXISTS {table_name}")
  # dbutils.fs.rm(checkpoint_path, True)

  # Configure Auto Loader to ingest JSON data to a UC Delta table
  (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", checkpoint_path)
      .load(input_path)
      .withColumn("view_duration", col("view_duration").cast("double"))
      .withColumn("timestamp", col("timestamp").cast("timestamp"))
      .withColumn("visit_website", col("visit_website").cast("boolean"))
      .withColumn("reaction", col("reaction").cast("string"))
      .withColumn("country", col("country").cast("string"))
      .withColumn("source_file", col("_metadata.file_path"))
      .withColumn("processing_time", current_timestamp())
      .writeStream
      .option("checkpointLocation", checkpoint_path)
      .trigger(availableNow=True)
      .toTable(table_name)
  )
