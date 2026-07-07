from pyspark import pipelines as dp

@dp.table(
    name="bronze_gdrive_transactions",
    comment="Ingest transactions from Google Drive into a bronze table.",
    table_properties={"quality": "bronze"}
)

def gdrive_bronze_table():
  return (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("databricks.connection", "googledriveconnection")
      .option("pathGlobFilter", "*.csv")
      .option("inferColumnTypes", True)
      .option("header", True)
      .load("https://drive.google.com/drive/folders/...")
      )