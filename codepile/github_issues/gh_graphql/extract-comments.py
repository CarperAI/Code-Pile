from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, filter, size, transform

spark_dir = "/data/tmp/"
spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", spark_dir).config("spark.driver.memory", "8G").config("spark.executor.cores", 10).master("local[16]").appName('spark-stats').getOrCreate()
df = spark.read.json("/data/comments-*.jsonl")

df2 = df.select(["data.repository.issues.pageInfo.hasNextPage", explode("data.repository.issues.edges").alias("issue")])
df3 = df2.select([
    col("issue.node.number").alias("issue_no"), 
    col("issue.node.databaseId").alias("issue_id"),
    col("issue.node.createdAt").alias("issue_created_at"),
    col("issue.node.comments.pageInfo.hasNextPage").alias("has_more_comments"),
    col("issue.node.comments.pageInfo.endCursor").alias("next_comments_cursor"),
    explode("issue.node.comments.nodes").alias("comment")
])

def filter_reactions(x):
    return x.reactors.totalCount > 0

def transform_reactions(x):
    print(x)
    return {x.content: x.reactors.totalCount}

df4 = df3.select([
    "issue_no",
    "issue_id",
    "issue_created_at",
    "has_more_comments",
    "next_comments_cursor",
    "comment.databaseId",
    "comment.authorAssociation",
    col("comment.author.login").alias("comment_author"),
    col("comment.body").alias("comment_body"),
    filter("comment.reactionGroups", filter_reactions)
        .alias("reaction_groups")
]).dropDuplicates(["databaseId"])

df4.write.parquet("/data/comments")