# Databricks notebook source
storage_account = "storageaccount350"
container="source"

client_id = dbutils.secrets.get(
    scope="databrickscope",
    key="clientid"
)

client_secret = dbutils.secrets.get(
    scope="databrickscope",
    key="clientsecret"
)

tenant_id = dbutils.secrets.get(
    scope="databrickscope",
    key="tenantid"
)

host = f"{storage_account}.dfs.core.windows.net"

spark.conf.set(
    f"fs.azure.account.auth.type.{host}",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{host}",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{host}",
    client_id
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{host}",
    client_secret
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{host}",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

# COMMAND ----------

source_container = "source"
bronze_container = "bronze"

base_source = f"abfss://{source_container}@{storage_account}.dfs.core.windows.net"
base_bronze = f"abfss://source@{storage_account}.dfs.core.windows.net/bronze"

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

#create batchid
from datetime import datetime

batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

print("Batch ID:", batch_id)

# COMMAND ----------

sales_2015 = (
    spark.read
    .option("header", True)
    .csv(f"{base_source}/AdventureWorks_Sales_2015.csv")
)

display(sales_2015)

# COMMAND ----------

display(dbutils.fs.ls(base_source))

# COMMAND ----------

sales_2016 = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Sales_2016.csv"
)

sales_2017 = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Sales_2017.csv"
)

returns = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Returns.csv"
)

customers = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Customers.csv"
)

products = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Products.csv"
)

product_subcategories = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Product_Subcategories.csv"
)

product_categories = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Product_Categories.csv"
)

territories = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Territories.csv"
)

calendar = spark.read.option("header", True).csv(
    f"{base_source}/AdventureWorks_Calendar.csv"
)

# COMMAND ----------

sales_2015 = (
    sales_2015
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Sales_2015.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

# COMMAND ----------

sales_2016 = (
    sales_2016
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Sales_2016.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

sales_2017 = (
    sales_2017
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Sales_2017.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

returns = (
    returns
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Returns.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

customers = (
    customers
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Customers.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

products = (
    products
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Products.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

product_subcategories = (
    product_subcategories
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Product_Subcategories.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

product_categories = (
    product_categories
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Product_Categories.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

territories = (
    territories
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Territories.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

calendar = (
    calendar
    .withColumn("_ingestion_date", current_timestamp())
    .withColumn("_source_file", lit("AdventureWorks_Calendar.csv"))
    .withColumn("_batch_id", lit(batch_id))
)

# COMMAND ----------

display(sales_2015)

# COMMAND ----------

display(calendar)

# COMMAND ----------

sales_2015.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.sales_2015")


# COMMAND ----------

sales_2016.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.sales_2016")
sales_2017.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.sales_2017")
returns.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.returns")

customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.customers")

products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.products")

product_subcategories.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.product_subcategories")

product_categories.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.product_categories")

territories.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.territories")

calendar.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.calendar")

# COMMAND ----------

display(calendar)

# COMMAND ----------

