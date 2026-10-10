# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

storage_account = "storageaccount350"



# COMMAND ----------

sales_2015 = spark.table("bronze.sales_2015")
sales_2016 = spark.table("bronze.sales_2016")
sales_2017 = spark.table("bronze.sales_2017")

# COMMAND ----------

sales = (
    sales_2015
    .unionByName(sales_2016)
    .unionByName(sales_2017)
)

# COMMAND ----------

print("Total sales rows:", sales.count())

# COMMAND ----------

sales_silver = (
    sales
    .withColumn(
        "OrderDate",
        to_date(col("OrderDate"), "M/d/yyyy")
    )
    .withColumn(
        "StockDate",
        to_date(col("StockDate"), "M/d/yyyy")
    )
    .withColumn(
        "OrderQuantity",
        col("OrderQuantity").cast("int")
    )
    .withColumn(
        "SalesOrderLineKey",
        concat_ws(
            "-",
            col("OrderNumber"),
            col("OrderLineItem")
        )
    )
    .dropDuplicates(["SalesOrderLineKey"])
)

# COMMAND ----------

sales_silver.printSchema()

# COMMAND ----------

sales_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.sales")


# COMMAND ----------

returns_bronze = spark.table("bronze.returns")

# COMMAND ----------

print("Returns rows:", returns_bronze.count())
returns_bronze.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, concat_ws, sha2

returns_silver = (
    returns_bronze
    .withColumn("ReturnDate", to_date(col("ReturnDate"), "M/d/yyyy"))
    .withColumn("ReturnQuantity", col("ReturnQuantity").cast("int"))
    .withColumn(
        "ReturnsLineKey",
        sha2(
            concat_ws(
                "|",
                col("ReturnDate").cast("string"),
                col("TerritoryKey"),
                col("ProductKey"),
                col("ReturnQuantity").cast("string")
            ),
            256
        )
    )
)

# COMMAND ----------

returns_silver.orderBy("ReturnsLineKey").show(8, truncate=False)

# COMMAND ----------

returns_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.returns")

# COMMAND ----------

customers = spark.table("bronze.customers")

display(customers)


# COMMAND ----------

customers_silver = (
    customers

    # Convert BirthDate to date
    .withColumn(
        "BirthDate",
        to_date(col("BirthDate"), "M/d/yyyy")
    )

    # Clean AnnualIncome
    .withColumn(
        "AnnualIncome",
        regexp_replace(
            regexp_replace(col("AnnualIncome"), "\\$", ""),
            ",",
            ""
        ).cast("decimal(12,2)")
    )

    # Trim text columns
    .withColumn("Prefix", trim(col("Prefix")))
    .withColumn("FirstName", trim(col("FirstName")))
    .withColumn("LastName", trim(col("LastName")))
    .withColumn("Gender", trim(col("Gender")))

    # Standardize names
    .withColumn("FirstName", initcap(col("FirstName")))
    .withColumn("LastName", initcap(col("LastName")))

    # Handle nulls
    .withColumn(
        "Prefix",
        coalesce(col("Prefix"), lit("Unknown"))
    )
    .withColumn(
        "Gender",
        coalesce(col("Gender"), lit("Unknown"))
    )

    # Remove duplicate customers
    .dropDuplicates(["CustomerKey"])
)

# COMMAND ----------

display(customers_silver)

# COMMAND ----------

customers_silver.printSchema()

# COMMAND ----------

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.customers")

# COMMAND ----------


products = spark.table("bronze.customers")

display(products)

# COMMAND ----------

products.printSchema()

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, coalesce, lit, regexp_replace, when
)

# Step 1: Read the Bronze Products table
products_bronze = spark.table("bronze.products")

# Step 2: Clean and transform the Products data
products_silver = (
    products_bronze

    # Replace null ProductColor values with "N/A"
    .withColumn(
        "ProductColor",
        coalesce(trim(col("ProductColor")), lit("N/A"))
    )

    # Convert ProductCost to decimal
    .withColumn(
        "ProductCost",
        regexp_replace(
            trim(col("ProductCost")), "[$,]", ""
        ).cast("decimal(12,2)")
    )

    # Convert ProductPrice to decimal
    .withColumn(
        "ProductPrice",
        regexp_replace(
            trim(col("ProductPrice")), "[$,]", ""
        ).cast("decimal(12,2)")
    )

    # Convert ProductSize placeholder "0" to null
    .withColumn(
        "ProductSize",
        when(
            trim(col("ProductSize")) == "0", None
        ).otherwise(trim(col("ProductSize")))
    )

    # Convert ProductStyle placeholder "0" to null
    .withColumn(
        "ProductStyle",
        when(
            trim(col("ProductStyle")) == "0", None
        ).otherwise(trim(col("ProductStyle")))
    )

    # Flag products where price is lower than cost
    .withColumn(
        "PriceCostAnomaly",
        when(
            col("ProductPrice").isNull()
            | col("ProductCost").isNull(),
            None
        ).otherwise(
            col("ProductPrice") < col("ProductCost")
        )
    )

    # Remove duplicate product keys
    .dropDuplicates(["ProductKey"])
)

# Step 3: Display the transformed data
display(products_silver)

# Step 4: Check the schema
products_silver.printSchema()

# Step 5: Data quality checks
print(
    "Total Silver Products rows:",
    products_silver.count()
)

print(
    "Products with missing or invalid cost/price:",
    products_silver.filter(
        col("ProductCost").isNull()
        | col("ProductPrice").isNull()
    ).count()
)

print(
    "Products where price is lower than cost:",
    products_silver.filter(
        col("PriceCostAnomaly") == True
    ).count()
)



# COMMAND ----------

display(products_silver)

# COMMAND ----------

products_silver.printSchema()

# COMMAND ----------

products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.products")

# COMMAND ----------

calendar = spark.table("bronze.calendar")

display(calendar)



# COMMAND ----------

calendar_silver = (
    calendar

    # Convert Date to date type
    .withColumn(
        "Date",
        to_date(col("Date"), "M/d/yyyy")
    )

    # Calendar attributes
    .withColumn("Year", year(col("Date")))
    .withColumn("Quarter", quarter(col("Date")))
    .withColumn("Month", month(col("Date")))
    .withColumn("MonthName", date_format(col("Date"), "MMMM"))
    .withColumn("DayOfWeek", date_format(col("Date"), "EEEE"))
    .withColumn("WeekOfYear", weekofyear(col("Date")))
    .withColumn(
        "IsWeekend",
        when(dayofweek(col("Date")).isin(1, 7), True)
        .otherwise(False)
    )

    # Remove duplicate dates
    .dropDuplicates(["Date"])
)

# COMMAND ----------

display(calendar_silver)

# COMMAND ----------

calendar_silver.printSchema()

# COMMAND ----------

calendar_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.calendar")
    

# COMMAND ----------

categories = spark.table("bronze.product_categories")

display(categories)


# COMMAND ----------

categories_silver = (
    categories

    .withColumn(
        "CategoryName",
        initcap(trim(col("CategoryName")))
    )

    .dropDuplicates(["ProductCategoryKey"])
)

# COMMAND ----------

display(categories_silver)

# COMMAND ----------

categories_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.product_categories")

# COMMAND ----------

subcategories = spark.table("bronze.product_subcategories")


# COMMAND ----------

subcategories_silver = (
    subcategories

    .withColumn(
        "SubcategoryName",
        initcap(trim(col("SubcategoryName")))
    )

    .dropDuplicates(["ProductSubcategoryKey"])
)

# COMMAND ----------


display(subcategories_silver)


# COMMAND ----------

subcategories_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.product_subcategories")

# COMMAND ----------

territories = spark.table("bronze.territories")

display(territories)



# COMMAND ----------

territories_silver = (
    territories

    .withColumn("Region", initcap(trim(col("Region"))))
    .withColumn("Country", initcap(trim(col("Country"))))
    .withColumn("Continent", initcap(trim(col("Continent"))))

    .dropDuplicates(["SalesTerritoryKey"])
)

# COMMAND ----------

display(territories_silver)


# COMMAND ----------

territories_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.territories")

# COMMAND ----------

