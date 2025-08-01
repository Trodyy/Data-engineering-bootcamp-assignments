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





--###PART 3 ###--
WITH yesterday AS (
	SELECT * FROM user_devices_cumulated
	WHERE date = DATE('2023-01-10')
) ,
	events_aggregated AS (
		SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY user_id , event_time) AS row_num
		FROM events
) ,
	events_deduped AS (
		SELECT * FROM events_aggregated WHERE row_num = 1
	) ,

	device_aggregated AS (
		SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY device_id , browser_type) AS row_num
		FROM devices
) ,
	device_deduped AS (
		SELECT * FROM device_aggregated WHERE row_num = 1
	) ,
	today AS (
		SELECT 
			user_id ,
			dd.browser_type ,
			DATE(event_time) AS event_time
		FROM events_deduped ed
		JOIN device_deduped dd ON ed.device_id = dd.device_id
		WHERE DATE(event_time) = DATE('2023-01-11')
		AND user_id IS NOT NULL
		GROUP BY (user_id , dd.browser_type , DATE(event_time))
) ,

 aggregated_data AS (SELECT
	COALESCE(t.user_id , y.user_id) AS user_id ,
	COALESCE(DATE(t.event_time) , y.date + INTERVAL '1 day') AS date ,
	COALESCE(t.browser_type , y.browser_type) AS browser_type ,
	-- ROW_NUMBER() OVER (PARTITION BY t.user_id , t.browser_type ,date)
	CASE
    WHEN y.device_activity_datelist IS NOT NULL AND t.event_time IS NOT NULL
        THEN y.device_activity_datelist || ARRAY[t.event_time]
    WHEN y.device_activity_datelist IS NOT NULL
        THEN y.device_activity_datelist -- Keep original without adding NULL
    ELSE
        ARRAY[t.event_time]
END AS device_activity_datelist
FROM 
today t FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id)



INSERT INTO user_devices_cumulated
SELECT * 
-- SELECT DISTINCT ON (user_id, browser_type, date) *
FROM aggregated_data
ON CONFLICT (user_id, browser_type) 
DO UPDATE SET 
    device_activity_datelist = EXCLUDED.device_activity_datelist;
