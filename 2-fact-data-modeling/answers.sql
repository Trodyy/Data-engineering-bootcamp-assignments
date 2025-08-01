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
		WHERE user_id IS NOT NULL
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
			browser_type ,
			DATE(event_time) AS event_time
		FROM events_deduped ed
		JOIN device_deduped dd ON ed.device_id = dd.device_id
		WHERE DATE(event_time) = DATE('2023-01-11')
		GROUP BY (user_id , browser_type , DATE(event_time))
) , 

 aggregated_data AS (SELECT
	COALESCE(t.user_id , y.user_id) AS user_id ,
	COALESCE(DATE(t.event_time) , y.date + INTERVAL '1 day') AS date ,
	COALESCE(t.browser_type , y.browser_type) AS browser_type ,
	CASE
    WHEN y.device_activity_datelist IS NOT NULL AND t.event_time IS NOT NULL
        THEN y.device_activity_datelist || ARRAY[t.event_time]
    WHEN y.device_activity_datelist IS NOT NULL
        THEN y.device_activity_datelist
    WHEN t.event_time IS NOT NULL
        THEN ARRAY[t.event_time]
 END AS device_activity_datelist
FROM 
today t FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id)


INSERT INTO user_devices_cumulated
SELECT * 
FROM aggregated_data
ON CONFLICT (user_id, browser_type) 
DO UPDATE SET 
    device_activity_datelist = EXCLUDED.device_activity_datelist;












--### PART 4 ###--
WITH daily_agg AS (
	SELECT
	user_id ,
	EXTRACT(
    	DAY FROM DATE('2023-01-10') - d.valid_date) AS days_since,
	udc.device_activity_datelist @> ARRAY[DATE(d.valid_date)] AS is_active
	FROM user_devices_cumulated udc
	CROSS JOIN
	(SELECT * FROM generate_series('2023-01-01' , '2023-01-10' , INTERVAL '1 day') AS valid_date) AS d
	-- WHERE date = DATE('2023-01-10')
	) ,
	bits AS (
	SELECT 
	user_id ,
	SUM(CASE
			WHEN is_active THEN POW(2 , 9 - days_since)
			ELSE 0
			END
	)::BIGINT::BIT(10) AS datelist_int ,
	ROW_NUMBER() OVER (PARTITION BY user_id)
	FROM daily_agg
	GROUP BY user_id
	)
SELECT
	user_id ,
	datelist_int ,
	BIT_COUNT(bits.datelist_int & CAST('1000000000' AS BIT(10))) > 0 AS last_day_active ,
	BIT_COUNT(bits.datelist_int & CAST('1111111000' AS BIT(10))) > 0 AS last_week_active
FROM bits










--###PART 5 ###--
CREATE TABLE hosts_cumulated (
	host_name TEXT ,
	host_activity_datelist DATE[] ,
	date DATE ,
	PRIMARY KEY (host_name)
)









--###PART 6 ###--
WITH events_aggregated AS (
		SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY user_id , event_time) AS row_num
		FROM events
		WHERE user_id IS NOT NULL 
) ,
	events_deduped AS (
		SELECT * FROM events_aggregated WHERE row_num = 1
	)  ,
	today AS (
		SELECT * FROM events_deduped WHERE DATE(event_time)  =DATE('2023-01-19')
	) ,
	yesterday AS (
		SELECT * FROM hosts_cumulated WHERE date = DATE('2023-01-18')
	) ,
	hosts_agg AS(
	SELECT host , DATE(event_time) AS date , COUNT(1) 
	FROM today 
	GROUP BY (host , DATE(event_time))
)
INSERT INTO hosts_cumulated
SELECT 
	COALESCE(hg.host , y.host_name) ,
	CASE 
		WHEN y.host_activity_datelist IS NOT NULL
			THEN y.host_activity_datelist || ARRAY[DATE(hg.date)]
		WHEN y.host_activity_datelist IS NULL
			THEN ARRAY[DATE(hg.date)]
			ELSE ARRAY[]::DATE[]
		END AS host_activity_datelist ,
	COALESCE(hg.date , y.date + INTERVAL '1 day') AS date
FROM hosts_agg hg FULL OUTER JOIN yesterday y ON hg.host = y.host_name
ON CONFLICT (host_name)
DO
	UPDATE
		SET host_activity_datelist = EXCLUDED.host_activity_datelist , date = EXCLUDED.date
