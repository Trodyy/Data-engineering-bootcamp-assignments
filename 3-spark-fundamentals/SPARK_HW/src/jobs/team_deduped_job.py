from pyspark.sql import SparkSession

query = """
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
    FROM teams
)
"""


def do_team_deduped(spark , df) :
    df.createOrReplaceTemp("team_deduped")
    spark.sql(query)


def main() :
    spark = SparkSession.builder \
    .appName("team_deduped") \
    .getOrCreate()
    output_df = do_team_deduped(spark , spark.table("teams"))
    output_df = do_team_deduped(spark, spark.table("teams"))
    output_df.write.mode("overwrite").insertInto("teams_deduped")
    