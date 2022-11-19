from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, filter, size, transform

spark_dir = "/data/tmp/"
spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", spark_dir).config("spark.driver.memory", "8G").config("spark.executor.cores", 10).master("local[16]").appName('spark-stats').getOrCreate()

def labels_transformer(x):
    return {"name": x.node.name, "description": x.node.description}

def filter_reactions(x):
    return x.reactors.totalCount > 0    

df = spark.read.json("/data/issues.jsonl")

# separate issues into their own rows
df2 = df.select([
    col("data.repository.databaseId").alias("repo_id"),
    col("data.repository.nameWithOwner").alias("repo_name_with_owner"),
    col("data.repository.stargazerCount").alias("star_count"),
    col("data.repository.description").alias("repo_description"),
    col("data.repository.languages.edges").alias("languages"),
    "data.repository.issues.pageInfo.hasNextPage",
    col("data.repository.issues.totalCount").alias("issue_count"),
    explode("data.repository.issues.edges").alias("issue")
])

#extract, clean issue metadata
df3 = df2.select([
    "repo_id",
    "repo_name_with_owner",
    "star_count",
    "repo_description",
    "languages",
    "issue_count",
    col("issue.node.number").alias("issue_no"), 
    col("issue.node.databaseId").alias("issue_id"),
    col("issue.node.createdAt").alias("issue_created_at"),
    col("issue.node.title").alias("title"),
    col("issue.node.author.login").alias("author"),
    col("issue.node.author.avatarUrl").alias("author_avatar"),
    col("issue.node.author.__typename").alias("author_type"),
    col("issue.node.authorAssociation").alias("author_association"),
    col("issue.node.comments.totalCount").alias("comment_count"),
    col("issue.node.labels.edges").alias("labels"),
    filter("issue.node.reactionGroups", filter_reactions)
        .alias("reaction_groups")
]).dropDuplicates(["issue_id"])
df3.write.parquet("/data/issues")
print(df3.count())