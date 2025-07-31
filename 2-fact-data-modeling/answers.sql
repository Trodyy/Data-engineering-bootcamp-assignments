--###PART 1 ###--
WITH deduped AS (
	SELECT 
	* ,
	ROW_NUMBER() OVER (PARTITION BY player_id , team_id , game_id) AS row_num
FROM game_details
)
SELECT * FROM deduped
WHERE row_num = 1






--###PART 2 ###--
CREATE TABLE user_devices_cumulated (
	user_id TEXT ,
	date DATE ,
	browser_type TEXT ,
	device_activity_datelist DATE ,
	PRIMARY KEY (user_id , browser_type)
)





