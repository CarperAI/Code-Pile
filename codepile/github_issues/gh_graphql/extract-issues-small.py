from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, filter, size, transform

spark_dir = "/data/tmp/"
spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", spark_dir).config("spark.driver.memory", "8G").config("spark.executor.cores", 10).master("local[16]").appName('spark-stats').getOrCreate()

def labels_transformer(x):
    return {"name": x.node.name, "description": x.node.description}

def filter_reactions(x):
    return x.reactors.totalCount > 0    

df = spark.read.json("/data/issues-*.jsonl")

# separate issues into their own rows
df2 = df.select([
    col("data.repository.databaseId").alias("repo_id"),
    col("data.repository.nameWithOwner").alias("repo_name_with_owner"),
    explode("data.repository.issues.edges").alias("issue")
])

#extract, clean issue metadata
df3 = df2.select([
    "repo_id",
    "repo_name_with_owner",
    col("issue.node.number").alias("issue_no"), 
    col("issue.node.databaseId").alias("issue_id"),
    col("issue.node.createdAt").alias("issue_created_at"),
    col("issue.node.title").alias("title"),
    col("issue.node.body").alias("body")
]).dropDuplicates(["issue_id"])
df3.write.parquet("/data/issues-lite")
print(df3.count())