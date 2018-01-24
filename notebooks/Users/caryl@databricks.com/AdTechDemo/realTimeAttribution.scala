// Databricks notebook source
// MAGIC %md # Reading Impressions and Conversions from Kinesis
// MAGIC 
// MAGIC ### Overview
// MAGIC In digital advertising, one of the most important things to be able to deliver to clients is information about how their advertising spend drove results -- and the more quickly we can provide this to clients, the better. To tie conversions to the impressions served in an advertising campaign, companies must perform attribution. Attribution can be a fairly expensive process, and running attribution against constantly updating datasets is challenging without the right technology.
// MAGIC 
// MAGIC Fortunately, Databricks makes this easy with Structured Streaming and Delta.
// MAGIC 
// MAGIC In this notebook we are going to take a quick look at how to use DataFrame API to build Structured Streaming applications on top of Kinesis, and using Delta to query the streams in near-real time.
// MAGIC ![stream](files/adtech-streaming.png)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

dbutils.fs.rm("/tmp/adtech/impressions", true)
dbutils.fs.rm("/tmp/adtech/impressions-checkpoints", true)
dbutils.fs.rm("/tmp/adtech/conversions", true)
dbutils.fs.rm("/tmp/adtech/conversions-checkpoints", true)

// COMMAND ----------

// MAGIC %run ./creds

// COMMAND ----------

val kinesis = spark.readStream
  .format("kinesis")
  .option("streamName", kinesisStreamName)
  .option("region", kinesisRegion)
  .option("initialPosition", "latest")
  .option("awsAccessKey", awsAccessKeyId)
  .option("awsSecretKey", awsSecretKey)
  .load()

val kinesisConv = spark.readStream
  .format("kinesis")
  .option("streamName", "adtech-conv")
  .option("region", kinesisRegion)
  .option("initialPosition", "latest")
  .option("awsAccessKey", awsAccessKeyId)
  .option("awsSecretKey", awsSecretKey)
  .load()

val schema = StructType(Seq( 
  StructField("uid", StringType, true),
  StructField("impTimestamp", TimestampType, true),
  StructField("exchangeID", IntegerType, true),  
  StructField("publisher", StringType, true),
  StructField("creativeID", IntegerType, true),
  StructField("click", StringType, true),
  StructField("advertiserID", IntegerType, true),
  StructField("browser", StringType, true),
  StructField("geo", StringType, true),
  StructField("bidAmount", DoubleType, true)
))

val schemaConv = StructType(Seq(
  StructField("uid", StringType, true),
  StructField("convTimestamp", TimestampType, true), 
  StructField("conversionID", IntegerType, true),
  StructField("advertiserID", StringType, true),
  StructField("pixelID", StringType, true),
  StructField("conversionValue", DoubleType, true)
))


// COMMAND ----------

val imp = kinesis.select(from_json('data.cast("string"), schema) as "fields").select($"fields.*")
val conv = kinesisConv.select(from_json('data.cast("string"), schemaConv) as "fields").select($"fields.*")
display(imp)

// COMMAND ----------

display(conv)

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
// import org.apache.spark.sql.functions._

// PERSIST IMPRESSION STREAM TO DELTA
imp.withWatermark("impTimestamp", "1 minute")
  .repartition(1)
  .writeStream
  .format("delta")
  .option("path","/tmp/adtech/impressions")
  .option("checkpointLocation","/tmp/adtech/impressions-checkpoints")
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds")).start()

conv.withWatermark("convTimestamp", "1 minute")
  .repartition(1)
  .writeStream
  .format("delta")
  .option("path","/tmp/adtech/conversions")
  .option("checkpointLocation","/tmp/adtech/conversions-checkpoints")
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds")).start()

// COMMAND ----------

// MAGIC %sql use adtech;
// MAGIC create or replace view impressionsDelta as select * from delta.`/tmp/adtech/impressions`;
// MAGIC create or replace view conversionsDelta as select * from delta.`/tmp/adtech/conversions`;

// COMMAND ----------

// MAGIC %sql drop view if exists global_temp.attributionDelta

// COMMAND ----------

val deltaImps = spark.sql("select uid as impUid, advertiserID as impAdv, * from impressionsDelta")
val deltaConvs = spark.sql("select * from conversionsDelta")
val windowSpec = Window.partitionBy("impUid").orderBy(desc("impTimestamp"))
val windowedAttribution = deltaConvs
  .join(deltaImps, deltaImps.col("impUid") === deltaConvs.col("uid") 
        && deltaImps.col("impTimestamp") < deltaConvs.col("convTimestamp"))
  .withColumn("dense_rank", dense_rank().over(windowSpec))
  .filter($"dense_rank"===1)

windowedAttribution.createGlobalTempView("attributionDelta")

//windowedAttribution.write.format("delta").save("/tmp/myNewDeltaTable")

// COMMAND ----------

// MAGIC %sql select * from global_temp.attributionDelta

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![my_test_image](files/dbtableau2.png)
// MAGIC 
// MAGIC * **Server:** demo.cloud.databricks.com
// MAGIC * **Port:** 443
// MAGIC * **Protocol:** HTTPS
// MAGIC * **HTTP Path:** See cluster page.