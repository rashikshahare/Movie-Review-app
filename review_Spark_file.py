# Databricks notebook source
import pandas as pd

rating_df= pd.read_csv("https://rashstorage2.blob.core.windows.net/rashcontainer/ratings.csv")

rating_df.display()

# COMMAND ----------

import pandas as pd

movies_df = pd.read_csv('https://rashstorage2.blob.core.windows.net/rashcontainer/movies.csv')

movies_df.display()

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

movies_df= spark.createDataFrame(movies_df)
movies_df.display(5)




# COMMAND ----------

rating_df= spark.createDataFrame(rating_df)
rating_df.display(5)

# COMMAND ----------

from pyspark.sql.functions import to_date

rating_df = rating_df.withColumn("date", to_date(rating_df["timestamp"]))

rating_df.show()

# COMMAND ----------

from pyspark.sql.functions import *

rating_df= rating_df.withColumn('timestamp', rating_df['timestamp'].cast('timestamp'))

rating_df.printSchema()

# COMMAND ----------

rating_df.display()

# COMMAND ----------

rating_df= rating_df.withColumn('date', to_date(rating_df['timestamp']))

rating_df.display()

# COMMAND ----------

rating_df= rating_df.withColumn('date', to_date(rating_df['timestamp']))
rating_df= rating_df.withColumn('time', date_format('timestamp', 'HH:mm:ss'))
rating_df= rating_df.drop('timestamp')
rating_df.show()

# COMMAND ----------

review_df= rating_df.join(movies_df, on='movieId', how='left')

review_df.show()

# COMMAND ----------

movies_df.show(5)

# COMMAND ----------

#movies_df = movies_df.withColumn("title1", regexp_extract("title", r"^(.*)\s\(\d{4}\)$", 1))
      

#movies_df.show()

# COMMAND ----------

movies_df = movies_df.withColumn("title1", regexp_extract("title", r"^(.*)\s\(\d{4}\)$", 1))\
       .withColumn("year", regexp_extract("title", r"\((\d{4})\)", 1))
      

movies_df.show()

# COMMAND ----------

movies_df= movies_df.drop("title")

movies_df.show()

# COMMAND ----------

movies_df = movies_df.withColumnRenamed("title1", "title")

movies_df.show()

# COMMAND ----------

review_df= review_df.drop("timestamp")

review_df.show()

# COMMAND ----------

review_df= review_df.join(movies_df, on='movieId', how='left')

review_df.show()

# COMMAND ----------

allreviews_df= rating_df.join(movies_df, on='movieId', how='left')

allreviews_df.show()

# COMMAND ----------

print(tabelName)

# COMMAND ----------

review_df.printSchema()


# COMMAND ----------

review_df = review_df.withColumn("timestamp", unix_timestamp("timestamp").cast("long"))

# Ensure correct data types
review_df = review_df.withColumn("userId", review_df["userId"].cast("integer"))
review_df = review_df.withColumn("movieId", review_df["movieId"].cast("integer"))
review_df = review_df.withColumn("rating", review_df["rating"].cast("float"))

review_df.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://<your_server>;loginTimeout=30") \
    .option("dbtable", "reviews") \
    .option("user", "rashadmin") \
    .option("password", "1234@Ra$hik") \
    .save()

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialise Spark session
spark = SparkSession.builder.appName("AzureSqlConnection").getOrCreate()

# Define JDBC connection properties
database = "rashdatabase"
username = "rashadmin"
password = "1234@Ra$hik"
jdbcUrl = f"jdbc:sqlserver://rashserveroct.database.windows.net:1433;database={database};user={username}@rashserveroct;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Connection properties
connectionProperties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Assume review_df is already defined above

# Define the table name correctly
table_name = "reviews"

# Write DataFrame to Azure SQL Server
review_df.write.jdbc(url=jdbcUrl, table=table_name, mode="append", properties=connectionProperties)

print("Data written to Azure SQL Server")


# COMMAND ----------

jdbcUrl = f"jdbc:sqlserver://rashserveroct.database.windows.net:1433;database={database};user={username}@rashserveroct;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


# COMMAND ----------

review_df.write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "reviews") \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()

# COMMAND ----------

#from pyspark.sql import SparkSession

# Initialise Spark session
#spark = SparkSession.builder.appName("AzureSqlConnection").getOrCreate()

# Define JDBC connection properties
database = "rashdatabase"
username = "rashadmin"
password = "1234@Ra$hik"
jdbcUrl = f"jdbc:sqlserver://rashserveroct.database.windows.net:1433;database={database};user={username}@rashserveroct;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Connection properties
connectionProperties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Assume review_df is already defined above

# Define the table name correctly
table_name = "movies"

# Write DataFrame to Azure SQL Server
movies_df.write.jdbc(url=jdbcUrl, table=table_name, mode="overwrite", properties=connectionProperties)

print("Data written to Azure SQL Server")


# COMMAND ----------

#from pyspark.sql import SparkSession

# Initialise Spark session
#spark = SparkSession.builder.appName("AzureSqlConnection").getOrCreate()

# Define JDBC connection properties
database = "rashdatabase"
username = "rashadmin"
password = "1234@Ra$hik"
jdbcUrl = f"jdbc:sqlserver://rashserveroct.database.windows.net:1433;database={database};user={username}@rashserveroct;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Connection properties
connectionProperties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Assume review_df is already defined above

# Define the table name correctly
table_name = "allreviews"

# Write DataFrame to Azure SQL Server
allreviews_df.write.jdbc(url=jdbcUrl, table=table_name, mode="overwrite", properties=connectionProperties)

print("Data written to Azure SQL Server")


# COMMAND ----------

