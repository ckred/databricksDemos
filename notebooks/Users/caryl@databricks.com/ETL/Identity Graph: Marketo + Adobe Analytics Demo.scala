// Databricks notebook source
// MAGIC %md ##Load Adobe Analytics Log from Mounted S3 Bucket
// MAGIC In the cell below, we are reading a dummy Adobe Analytics CSV file from an S3 bucket that has been mounted to Databricks. As you can see, Spark can infer the schema automatically and display the results as a table directly in the notebook.

// COMMAND ----------

val adobe = spark.read
.option("header", true)
.csv("/mnt/databricks-caryl/politico/adobeAnalyticsLog.csv")

// COMMAND ----------

display(adobe)

// COMMAND ----------

// MAGIC %md ## Upload Marketo Data Set through UI
// MAGIC 
// MAGIC We used the Create Table UI to manually upload a dummy Marketo log CSV into a table called 'marketoLog'.
// MAGIC 
// MAGIC ![alt text](http://i.imgur.com/IDgVH03.png "Create Table")
// MAGIC ![alt text](http://imgur.com/3thcGVv.png "Upload marketoLog")
// MAGIC 
// MAGIC We can now query this table below.

// COMMAND ----------

// MAGIC %sql select * from marketo1

// COMMAND ----------

// MAGIC %md ## Join Adobe and Marketo Data Sets

// COMMAND ----------

adobe.write.saveAsTable("adobe1") // First, we'll create a global table from the Adobe dataframe that we created in the first step.

// COMMAND ----------

// MAGIC %sql drop table if exists marketoAdobe

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Now we can join the two datasets using SQL
// MAGIC create table marketoAdobe as (select adobe1.timestamp, adobe1.email_address, adobe1.cookie_id, marketo1.email_event, marketo1.template_id from adobe1, marketo1 where adobe1.email_address = marketo1.email_address)

// COMMAND ----------

display(sql("select * from marketoAdobe"))

// COMMAND ----------

val myDF = spark.sql("select * from marketoAdobe")
myDF.write.parquet("/mnt/databricks-caryl/tempFiles")

// COMMAND ----------

// MAGIC %py  df = spark.sql