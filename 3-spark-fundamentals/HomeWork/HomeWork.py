### Set up pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast , col , count , max
spark = SparkSession.builder.appName("HomeWork").getOrCreate()
spark
### Load data
match_details = spark.read.option("header" , "true").option("inferSchema" , "true").csv("/home/jovyan/work/match_details.csv")
matches = spark.read.option("header" , "true").option("inferSchema" , "true").csv("/home/jovyan/work/matches.csv")
matches.count()
medals = spark.read.option("header" , "true").option("inferSchema" , "true").csv("/home/jovyan/work/medals.csv")
medal_matches_players = spark.read.option("header" , "true").option("inferSchema" , "true").csv("/home/jovyan/work/medals_matches_players.csv")
medals.count()
maps = spark.read.option("header" , "true").option("inferSchema" , "true").csv("/home/jovyan/work/maps.csv")
### PART 1 - Disable automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
### Check columns for joining
medals.columns
maps.columns
### PART 2 - Broadcast Join 
combined = medals.join( broadcast(maps) , on="name")
combined.show(5)
### PART 3 - bucket join
import shutil

shutil.rmtree("/home/jovyan/work/spark-warehouse/bootcamp.db/matches_bucketed", ignore_errors=True)
shutil.rmtree("/home/jovyan/work/spark-warehouse/bootcamp.db/match_details_bucketed", ignore_errors=True)
shutil.rmtree("/home/jovyan/work/spark-warehouse/bootcamp.db/medal_matches_players_bucketed", ignore_errors=True)

matches.write \
  .bucketBy(16, "match_id") \
  .sortBy("match_id") \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("bootcamp.matches_bucketed")

match_details.write \
  .bucketBy(16, "match_id") \
  .sortBy("match_id") \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("bootcamp.match_details_bucketed")

medal_matches_players.write \
  .bucketBy(16, "match_id") \
  .sortBy("match_id") \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("bootcamp.medal_matches_players_bucketed")
matches_bucketed = spark.table("bootcamp.matches_bucketed")
match_details_bucketed = spark.table("bootcamp.match_details_bucketed")
medal_matches_players_bucketed = spark.table("bootcamp.medal_matches_players_bucketed")
joined_df = match_details_bucketed.join(
    matches_bucketed,
    "match_id",
    "inner" 
)

final_joined_df = joined_df.join(
    medal_matches_players_bucketed,
    "match_id",
    "inner"
)
final_joined_df.take(1)
### PART 4 SECTION 1 - Which player averages the most kills per game?
avg_kills_df = final_joined_df.groupBy("player_gamertag").agg(avg("player_total_kills").alias("avg_kills"))
avg_kills_df.orderBy(avg_kills_df["avg_kills"].desc()).show(1)
### PART 4 SECTION 2 - Which playlist gets played the most?
final_joined_df.groupBy("playlist_id") \
.agg(
    count("*").alias("playlist_counter")
) \
.orderBy(col("playlist_counter").desc()).show()
### PART 4 SECTION 3 - Which map gets played the most?
final_joined_df.groupBy("mapid") \
  .agg(
      count("*").alias("map_count")
  ) \
  .orderBy(col("map_count").desc()).show()
### PART 4 SECTION 4 - Which map do players get the most Killing Spree medals on?
from pyspark.sql.functions import col, count

# Replace 'Killing_Spree_ID' with the actual medal_id for Killing Spree
killing_spree_id = "Killing_Spree_ID"

# Filter for Killing Spree medals and count per map
final_joined_df.filter(col("medal_id") == killing_spree_id) \
    .groupBy("mapid") \
    .agg(count("*").alias("killing_spree_count")) \
    .orderBy(col("killing_spree_count").desc()) \
    .show(1)  # top 1 map

### PART 5 - sortWithPartitions
sortedViaMapID = final_joined_df.sortWithinPartitions(16 ,"mapid")
sortedViaPlaylist_ID = final_joined_df.sortWithinPartitions(16 ,"playlist_id")

