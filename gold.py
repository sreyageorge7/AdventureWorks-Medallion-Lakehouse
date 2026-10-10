# Databricks notebook source
from pyspark.sql.functions import *

# Load Silver tables
sales = spark.table("silver.sales")
returns = spark.table("silver.returns")
customers = spark.table("silver.customers")
products = spark.table("silver.products")
calendar = spark.table("silver.calendar")
territories = spark.table("silver.territories")
categories = spark.table("silver.product_categories")
subcategories = spark.table("silver.product_subcategories")


# COMMAND ----------

sales.printSchema()
products.printSchema()
customers.printSchema()
returns.printSchema()

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    concat_ws,
    trim,
    current_date,
    year,
    month,
    dayofmonth,
    when
)

# Step 1: Read the Silver Customers table
customers = spark.table("silver.customers")

# Step 2: Create FullName, Age, and AgeGroup
gold_dim_customer = (
    customers

    # Combine first name and last name
    .withColumn(
        "FullName",
        trim(concat_ws(" ", col("FirstName"), col("LastName")))
    )

    # Calculate accurate age using BirthDate
    .withColumn(
        "Age",
        when(
            col("BirthDate").isNull(),
            None
        ).otherwise(
            year(current_date()) - year(col("BirthDate"))
            - when(
                (month(current_date()) < month(col("BirthDate"))) |
                (
                    (month(current_date()) == month(col("BirthDate"))) &
                    (dayofmonth(current_date()) < dayofmonth(col("BirthDate")))
                ),
                1
            ).otherwise(0)
        )
    )

    # Group customers into age categories
    .withColumn(
        "AgeGroup",
        when(col("Age").isNull(), "Unknown")
        .when(col("Age") < 18, "Under 18")
        .when(col("Age") <= 29, "18-29")
        .when(col("Age") <= 39, "30-39")
        .when(col("Age") <= 49, "40-49")
        .when(col("Age") <= 59, "50-59")
        .otherwise("60+")
    )

    # Remove technical ingestion columns
    .drop("_ingestion_date", "_source_file", "_batch_id")

    # Keep one record per customer
    .dropDuplicates(["CustomerKey"])
)

# Step 3: Display the result
display(gold_dim_customer)

# Step 4: Save as a Gold Delta table
gold_dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.gold_dim_customer")

print("Gold Customer Dimension created successfully!")



# COMMAND ----------

from pyspark.sql.functions import col

# Step 1: Read Silver tables
products = spark.table("silver.products")
subcategories = spark.table("silver.product_subcategories")
categories = spark.table("silver.product_categories")

# Step 2: Join products with subcategories and categories
gold_dim_product = (
    products
    .join(
        subcategories,
        on="ProductSubcategoryKey",
        how="left"
    )
    .join(
        categories,
        on="ProductCategoryKey",
        how="left"
    )
    .select(
        products["ProductKey"],
        products["ProductSKU"],
        products["ProductName"],
        products["ModelName"],
        products["ProductDescription"],
        products["ProductColor"],
        products["ProductSize"],
        products["ProductStyle"],
        products["ProductCost"],
        products["ProductPrice"],
        subcategories["ProductSubcategoryKey"],
        subcategories["SubcategoryName"],
        categories["ProductCategoryKey"],
        categories["CategoryName"]
    )
    .dropDuplicates(["ProductKey"])
)

# Step 3: Display the result
display(gold_dim_product)

# Step 4: Save as a Gold table
gold_dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.gold_dim_product")

print("Gold Product Dimension created successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

# Step 1: Read the Silver Territories table
territories = spark.table("silver.territories")

# Step 2: Create the Gold Territory Dimension
gold_dim_territory = (
    territories
    .select("*")
    .drop("_ingestion_date", "_source_file", "_batch_id")
    .dropDuplicates()
)

# Step 3: Display the result
display(gold_dim_territory)

# Step 4: Save as a Gold table
gold_dim_territory.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.gold_dim_territory")

print("Gold Territory Dimension created successfully!")

# COMMAND ----------

from pyspark.sql.functions import (
    col, year, quarter, month, date_format,
    dayofmonth, dayofweek, weekofyear, when
)

# Step 1: Read the Silver Calendar table
calendar = spark.table("silver.calendar")

# Step 2: Create the Gold Date Dimension
gold_dim_date = (
    calendar
    .withColumn("Year", year(col("Date")))
    .withColumn("Quarter", quarter(col("Date")))
    .withColumn("Month", month(col("Date")))
    .withColumn("MonthName", date_format(col("Date"), "MMMM"))
    .withColumn("Day", dayofmonth(col("Date")))
    .withColumn("DayOfWeek", date_format(col("Date"), "EEEE"))
    .withColumn("WeekOfYear", weekofyear(col("Date")))
    .withColumn(
        "IsWeekend",
        when(dayofweek(col("Date")).isin(1, 7), True).otherwise(False)
    )
    .drop("_ingestion_date", "_source_file", "_batch_id")
    .dropDuplicates(["Date"])
)

# Step 3: Display the result
display(gold_dim_date)

# Step 4: Save as a Gold table
gold_dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.gold_dim_date")

print("Gold Date Dimension created successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

# Step 1: Read Silver tables
sales = spark.table("silver.sales")
products = spark.table("silver.products")

# Step 2: Join sales with products to get cost and price
gold_fact_sales = (
    sales
    .join(
        products.select(
            "ProductKey",
            "ProductPrice",
            "ProductCost"
        ),
        on="ProductKey",
        how="left"
    )

    # Step 3: Calculate sales metrics
    .withColumn(
        "UnitPrice",
        col("ProductPrice")
    )
    .withColumn(
        "UnitCost",
        col("ProductCost")
    )
    .withColumn(
        "SalesAmount",
        col("OrderQuantity") * col("ProductPrice")
    )
    .withColumn(
        "TotalCost",
        col("OrderQuantity") * col("ProductCost")
    )
    .withColumn(
        "GrossProfit",
        col("SalesAmount") - col("TotalCost")
    )

    # Step 4: Remove technical ingestion columns
    .drop(
        "_ingestion_date",
        "_source_file",
        "_batch_id",
        "ProductPrice",
        "ProductCost"
    )
)

# Step 5: Display the result
display(gold_fact_sales)

# Step 6: Save as a Gold table
gold_fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.gold_fact_sales")

print("Gold Sales Fact Table created successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

# Step 1: Read Silver tables
returns = spark.table("silver.returns")
products = spark.table("silver.products")

# Step 2: Join returns with products to get product price
gold_fact_returns = (
    returns
    .join(
        products.select(
            "ProductKey",
            "ProductPrice"
        ),
        on="ProductKey",
        how="left"
    )

    # Step 3: Calculate estimated return value
    .withColumn(
        "UnitPrice",
        col("ProductPrice")
    )
    .withColumn(
        "ReturnValue",
        col("ReturnQuantity") * col("ProductPrice")
    )

    # Step 4: Remove technical ingestion columns
    .drop(
        "_ingestion_date",
        "_source_file",
        "_batch_id",
        "ProductPrice"
    )
)

# Step 5: Display the result
display(gold_fact_returns)

# Step 6: Save as a Gold table
gold_fact_returns.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.gold_fact_returns")

print("Gold Returns Fact Table created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC --sales summary view
# MAGIC CREATE OR REPLACE VIEW gold.vw_sales_summary AS
# MAGIC SELECT
# MAGIC     s.OrderDate,
# MAGIC     s.OrderNumber,
# MAGIC     s.CustomerKey,
# MAGIC     c.FullName,
# MAGIC     c.Age,
# MAGIC     c.AgeGroup,
# MAGIC     s.ProductKey,
# MAGIC     p.ProductName,
# MAGIC     p.CategoryName,
# MAGIC     p.SubcategoryName,
# MAGIC     s.TerritoryKey,
# MAGIC     s.OrderQuantity,
# MAGIC     s.UnitPrice,
# MAGIC     s.SalesAmount,
# MAGIC     s.TotalCost,
# MAGIC     s.GrossProfit
# MAGIC FROM gold.gold_fact_sales s
# MAGIC LEFT JOIN gold.gold_dim_customer c
# MAGIC     ON s.CustomerKey = c.CustomerKey
# MAGIC LEFT JOIN gold.gold_dim_product p
# MAGIC     ON s.ProductKey = p.ProductKey;

# COMMAND ----------

# MAGIC %sql
# MAGIC --product performance view
# MAGIC CREATE OR REPLACE VIEW gold.vw_product_performance AS
# MAGIC SELECT
# MAGIC     ProductKey,
# MAGIC     ProductName,
# MAGIC     ProductSKU,
# MAGIC     CategoryName,
# MAGIC     SubcategoryName,
# MAGIC     ProductColor,
# MAGIC     ProductPrice,
# MAGIC     ProductCost,
# MAGIC     ProductPrice - ProductCost AS UnitProfit,
# MAGIC     CASE
# MAGIC         WHEN ProductPrice > 0
# MAGIC         THEN ROUND(
# MAGIC             ((ProductPrice - ProductCost) / ProductPrice) * 100,
# MAGIC             2
# MAGIC         )
# MAGIC         ELSE NULL
# MAGIC     END AS ProfitMarginPercent
# MAGIC FROM gold.gold_dim_product;

# COMMAND ----------

# MAGIC %sql
# MAGIC --customer analysis view
# MAGIC CREATE OR REPLACE VIEW gold.vw_customer_analysis AS
# MAGIC SELECT
# MAGIC     CustomerKey,
# MAGIC     FullName,
# MAGIC     Age,
# MAGIC     AgeGroup,
# MAGIC     Gender,
# MAGIC     MaritalStatus,
# MAGIC     EducationLevel,
# MAGIC     Occupation,
# MAGIC     AnnualIncome,
# MAGIC     TotalChildren,
# MAGIC     HomeOwner
# MAGIC FROM gold.gold_dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC --territory sales view
# MAGIC CREATE OR REPLACE VIEW gold.vw_territory_sales AS
# MAGIC SELECT
# MAGIC     s.TerritoryKey,
# MAGIC     t.* EXCEPT (TerritoryKey),
# MAGIC     SUM(s.SalesAmount) AS TotalSales,
# MAGIC     SUM(s.GrossProfit) AS TotalGrossProfit,
# MAGIC     SUM(s.OrderQuantity) AS TotalQuantity,
# MAGIC     COUNT(DISTINCT s.OrderNumber) AS TotalOrders
# MAGIC FROM gold.gold_fact_sales s
# MAGIC LEFT JOIN gold.gold_dim_territory t
# MAGIC     ON s.TerritoryKey = t.TerritoryKey
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --returns analysis view 
# MAGIC CREATE OR REPLACE VIEW gold.vw_returns_analysis AS
# MAGIC SELECT
# MAGIC     r.ReturnDate,
# MAGIC     r.ProductKey,
# MAGIC     p.ProductName,
# MAGIC     p.CategoryName,
# MAGIC     r.TerritoryKey,
# MAGIC     r.ReturnQuantity,
# MAGIC     r.UnitPrice,
# MAGIC     r.ReturnValue
# MAGIC FROM gold.gold_fact_returns r
# MAGIC LEFT JOIN gold.gold_dim_product p
# MAGIC     ON r.ProductKey = p.ProductKey;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS IN gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.vw_sales_summary
# MAGIC LIMIT 10;

# COMMAND ----------

