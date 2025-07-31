--###PART 1 ###--
WITH deduped AS (
	SELECT 
	* ,
	ROW_NUMBER() OVER (PARTITION BY player_id , team_id , game_id) AS row_num
FROM game_details
)
SELECT * FROM deduped
WHERE row_num = 1
