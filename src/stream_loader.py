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
      .select("*", 
              col("_metadata.file_path").alias("source_file"), 
              current_timestamp().alias("processing_time"))
      .writeStream
      .option("checkpointLocation", checkpoint_path)
      .trigger(availableNow=True)
      .toTable(table_name)
  )