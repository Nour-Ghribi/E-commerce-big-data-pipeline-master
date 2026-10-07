from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, to_date, date_format, monotonically_increasing_id
from pyspark.sql.types import DateType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# Read parquet files from HDFS
df = spark.read.parquet("hdfs://namenode:9000/user/root/nader/data/tmpdata/*.parquet", header=True, inferSchema=True)

# Data Cleaning

# Drop rows with nulls
df_cleaned = df.dropna()

# Trim whitespace from string columns
df_cleaned = df_cleaned.select(
    *[trim(col(c)).alias(c) if dict(df_cleaned.dtypes).get(c) == 'string' else col(c) for c in df_cleaned.columns]
)

# Remove duplicates
df_cleaned = df_cleaned.dropDuplicates()

# Convert column names to lower case and replace spaces with underscores
df_cleaned = df_cleaned.select(
    *[col(c).alias(c.lower().replace(" ", "_")) for c in df_cleaned.columns]
)

# Clean non-ASCII characters in string columns
string_cols = [f.name for f in df_cleaned.schema.fields if f.dataType.simpleString() == 'string']
for c in string_cols:
    df_cleaned = df_cleaned.withColumn(c, regexp_replace(col(c), r"[^\x20-\x7E]", ""))

# Convert date/timestamp columns to unified date format
date_cols = [f.name for f in df_cleaned.schema.fields if isinstance(f.dataType, (DateType, TimestampType))]
for col_name in date_cols:
    df_cleaned = df_cleaned.withColumn(col_name, to_date(col(col_name), 'yyyy-MM-dd'))
    df_cleaned = df_cleaned.withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd"))

# DIM PRODUCT
dim_product = df_cleaned.select("product_id", "category_id", "category_code", "brand") \
    .dropDuplicates() \
    .withColumn("product_sk", monotonically_increasing_id())

# DIM USER
dim_user = df_cleaned.select("user_id").dropDuplicates() \
    .withColumn("user_sk", monotonically_increasing_id())

# DIM CATEGORY
dim_category = df_cleaned.select("category_id", "category_code").dropDuplicates() \
    .withColumn("category_sk", monotonically_increasing_id())

# Fact table join with dimension tables
fact_events_fk = df_cleaned \
    .join(dim_product.select("product_id", "product_sk"), on="product_id", how="left") \
    .join(dim_user.select("user_id", "user_sk"), on="user_id", how="left") \
    .join(dim_category.select("category_id", "category_sk"), on="category_id", how="left") \
    .select("event_time", "event_type", "product_sk", "user_sk", "category_sk", "price", "user_session")

# Save dim_product to PostgreSQL
dim_product.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://citus_master:5432/postgres") \
    .option("dbtable", "dim_product") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Save dim_user to PostgreSQL
dim_user.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://citus_master:5432/postgres") \
    .option("dbtable", "dim_user") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Save dim_category to PostgreSQL
dim_category.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://citus_master:5432/postgres") \
    .option("dbtable", "dim_category") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Save fact_events to PostgreSQL
fact_events_fk.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://citus_master:5432/postgres") \
    .option("dbtable", "fact_events") \
    .option("user", "postgres") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
