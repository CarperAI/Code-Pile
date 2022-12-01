from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, filter, size, transform, lit, create_map, collect_list, to_json
spark_dir = "/tmp/"
spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", spark_dir).config("spark.driver.memory", "24G").config("spark.executor.cores", 10).master("local[16]").appName('spark-stats').getOrCreate()
issues = spark.read.parquet("/fsx/shared/codepile/github_issues/github-issues-all-filtered/")
comments_unfiltered = spark.read.parquet("/fsx/shared/codepile/github_issues/github-issues-comment-all/")

comments = comments_unfiltered.orderBy("event_created_at", ascending=False).dropDuplicates(["comment_id"])

def create_map_args(df):
    args = []
    for c in df.columns:
        args.append(lit(c))
        args.append(col(c))
    return args

comments_dicted = comments.withColumn("dict",
            create_map(create_map_args(comments.select(["comment_id", "comment"])))
        ).select(["issue_id", "issue_no", "dict"])

comments_grouped = comments_dicted.groupby(["issue_id", "issue_no"]).agg(collect_list("dict").alias("comments"))

print("Adding comments to issues")
issues_joined = issues.join(comments_grouped, issues.issue_id == comments_grouped.issue_id, "left").select(issues["*"], to_json(comments_grouped["comments"]).alias("comments"))