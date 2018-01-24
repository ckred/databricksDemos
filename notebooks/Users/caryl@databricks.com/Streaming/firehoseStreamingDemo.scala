// Databricks notebook source
// MAGIC %md # AWS Kinesis Firehose Streaming Demo
// MAGIC 
// MAGIC ![AWS Kinesis Firehose](http://i.imgur.com/rgpXaD5.png)
// MAGIC 
// MAGIC This notebook demonstrates how to ingest data from AWS Kinesis Firehose via Structured Streaming. In this example, the Kinesis Firehose writes to an S3 bucket specified in the 'inputPath' variable below. The data is demo market data, streamed into the S3 bucket by AWS.

// COMMAND ----------

// MAGIC %md ### Step 1: Specify the source and define the schema.
// MAGIC 
// MAGIC First, we need to tell Spark where to look for the data (inputPath).
// MAGIC 
// MAGIC Secondly, it's a requirement for Structured Streaming that the schema of the streaming data be pre-defined. In this case, these are the field and data types we expect from the dummy data stream. The format of the demo data is as follows: {"ticker_symbol":"QXZ", "sector":"HEALTHCARE", "change":-0.05, "price":84.51}

// COMMAND ----------

val inputPath = "/mnt/databricks-caryl/kinesis/2017/03/10/20/*"

// COMMAND ----------

import org.apache.spark.sql.types._
val jsonSchema = new StructType().add("change", DoubleType).add("price", DoubleType).add("sector", StringType).add("ticker_symbol", StringType)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Step 2: Create the stream reader and any operators.
// MAGIC 
// MAGIC The first readStream entity below pulls in the data from the input path and converts it into JSON format using the schema specified above.
// MAGIC  
// MAGIC We'll also create an operator that processes the data in that readStream. This operator essentially transforms the data into the insights we are looking for (in this case, the counts of price changes by sector).

// COMMAND ----------

val streamReader = spark.readStream
.schema(jsonSchema)
.option("maxFilesPerTrigger", 1)
.json(inputPath)

val streamingCounts = streamReader
.groupBy($"sector")
.count()

// COMMAND ----------

// MAGIC %md ### Step 3: Output the results.
// MAGIC The next step is to write the desired output from the stream either to memory for quick querying, or to a more permanent location for downstream use. *Note:* It is not recommended to write large output datasets to memory, as this obviously detracts from the processing performance.
// MAGIC 
// MAGIC Let's read the JSON objects from the stream and write them to a sink (in this case, a folder in an S3 bucket). The data can then be pulled from S3 the way batch files are loaded, queried and processed. This is even easier now that the data is available in a clean and performant Parquet format.

// COMMAND ----------

// Sink and Checkpoints folders must be empty for new streams.
dbutils.fs.rm("mnt/databricks-caryl/kinesis/sinkTableCheckpoints", true)
dbutils.fs.rm("mnt/databricks-caryl/kinesis/sinkTable", true)
dbutils.fs.rm("mnt/databricks-caryl/kinesis/bySectorCheckpoints", true)
dbutils.fs.rm("mnt/databricks-caryl/kinesis/bySectorTable", true)

// COMMAND ----------

val staticWriter = streamReader.writeStream
.format("parquet")
.option("path", "/mnt/databricks-caryl/kinesis/sinkTable")
.option("checkpointLocation", "/mnt/databricks-caryl/kinesis/sinkTableCheckpoints")
.outputMode("append")
.start()

// COMMAND ----------

// MAGIC %md Let's see what this looks like when we write to memory -- the data is more quickly available for querying, but it places a load on the resources processing the data. This is highly unfavorable when the dataset continues to grow, since it takes away from the available memory used for processing the stream. Eventually, this can cause the driver to run out of memory and fail.

// COMMAND ----------

val streamingWriter = streamReader.writeStream
.format("memory")
.queryName("inMemoryTable")
.outputMode("append")
.start()

// COMMAND ----------

Thread.sleep(120000)

// COMMAND ----------

// MAGIC %md ### Step 4: Analyze the data.
// MAGIC Now that the streams are running, we can begin to look at the data. Let's take a look at the sink data from our first writer.

// COMMAND ----------

// MAGIC %fs ls mnt/databricks-caryl/kinesis/sinkTable

// COMMAND ----------

val sinkDF = spark.read.parquet("mnt/databricks-caryl/kinesis/sinkTable")
display(sinkDF)

// COMMAND ----------

// MAGIC %md This should be comparable to the data we see when we query the table from the 2nd writer. You'll see however, that there is more data available to query in the in-memory storage. This is because it takes longer to convert the data to Parquet and write it to S3.

// COMMAND ----------

// MAGIC %sql select * from inMemoryTable limit 20

// COMMAND ----------

streamingWriter.stop
staticWriter.stop

// COMMAND ----------

// MAGIC %md Writing to memory is only recommended for debugging and testing. Let's say, for example, that we wanted to ensure the stream was live. We could start counting the records in memory. We can check to see this is working, then stop the stream and dump the in-memory table to optimize performance.

// COMMAND ----------

val inMemCounter = streamingCounts.writeStream
.format("memory")
.queryName("sectorCount")
// .option("path", "/mnt/databricks-caryl/kinesis/bySector")
// .option("checkpointLocation", "/mnt/databricks-caryl/kinesis/bySectorCheckpoints")
.outputMode("complete")
.start()

// COMMAND ----------

Thread.sleep(120000)

// COMMAND ----------

// MAGIC %sql select * from sectorCount

// COMMAND ----------

inMemCounter.stop()
spark.catalog.dropTempView("inMemCounter")