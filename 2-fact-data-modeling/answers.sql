--###PART 1 ###-- Deduplication of game details data
-- This CTE identifies and removes duplicate records from game_details table
-- by assigning row numbers within groups of (player_id, team_id, game_id)
-- and then keeping only the first occurrence of each group
WITH deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY player_id, team_id, game_id) AS row_num
    FROM game_details
)
SELECT * FROM deduped
WHERE row_num = 1  -- Only keep the first row of each group (deduplication)







--###PART 2 ###-- Create user devices cumulative table
-- Creates a table to track user device activity over time with:
-- user_id: Identifier for the user
-- date: The date of the record
-- browser_type: The browser being used
-- device_activity_datelist: An array tracking all dates the device was active
-- Primary key ensures one record per user-browser combination
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    date DATE,
    browser_type TEXT,
    device_activity_datelist DATE[],
    PRIMARY KEY (user_id, browser_type)
)









--###PART 3 ###-- User device activity tracking ETL process
-- This complex query updates the cumulative user device activity table by:
-- 1. Getting yesterday's data (from user_devices_cumulated)
-- 2. Processing today's events (deduplicating events and devices)
-- 3. Merging yesterday and today's data with proper array concatenation
-- 4. Handling conflicts with ON CONFLICT UPDATE
WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-10')  -- Get yesterday's snapshot
),
-- Deduplicate events by user and timestamp
events_aggregated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id, event_time) AS row_num
    FROM events
    WHERE user_id IS NOT NULL  -- Ensure we have valid users
),
events_deduped AS (
    SELECT * FROM events_aggregated WHERE row_num = 1  -- Keep only first occurrence
),
-- Deduplicate device information
device_aggregated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY device_id, browser_type) AS row_num
    FROM devices
),
device_deduped AS (
    SELECT * FROM device_aggregated WHERE row_num = 1  -- Keep only first occurrence
),
-- Get today's activity by joining deduplicated events with device info
today AS (
    SELECT 
        user_id,
        browser_type,
        DATE(event_time) AS event_time
    FROM events_deduped ed
    JOIN device_deduped dd ON ed.device_id = dd.device_id
    WHERE DATE(event_time) = DATE('2023-01-11')  -- Today's date
    GROUP BY (user_id, browser_type, DATE(event_time))  -- Ensure uniqueness
),
-- Combine yesterday and today's data
aggregated_data AS (
    SELECT
        COALESCE(t.user_id, y.user_id) AS user_id,
        COALESCE(DATE(t.event_time), y.date + INTERVAL '1 day') AS date,
        COALESCE(t.browser_type, y.browser_type) AS browser_type,
        CASE
            WHEN y.device_activity_datelist IS NOT NULL AND t.event_time IS NOT NULL
                THEN y.device_activity_datelist || ARRAY[t.event_time]  -- Append new date to array
            WHEN y.device_activity_datelist IS NOT NULL
                THEN y.device_activity_datelist  -- Keep existing array
            WHEN t.event_time IS NOT NULL
                THEN ARRAY[t.event_time]  -- Create new array for new users
        END AS device_activity_datelist
    FROM today t 
    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id  -- Preserve all records
)

-- Insert or update the cumulative table
INSERT INTO user_devices_cumulated
SELECT * 
FROM aggregated_data
ON CONFLICT (user_id, browser_type)  -- Handle duplicates
DO UPDATE SET 
    device_activity_datelist = EXCLUDED.device_activity_datelist;  -- Update array when conflict












--### PART 4 ###-- User activity analysis with bitmask pattern
-- Analyzes user activity patterns by:
-- 1. Creating a daily activity matrix
-- 2. Converting activity to a bitmask representation
-- 3. Calculating activity metrics from the bitmask
WITH daily_agg AS (
    SELECT
        user_id,
        EXTRACT(DAY FROM DATE('2023-01-10') - d.valid_date) AS days_since,  -- Days since each date
        udc.device_activity_datelist @> ARRAY[DATE(d.valid_date)] AS is_active  -- Check if active on date
    FROM user_devices_cumulated udc
    CROSS JOIN  -- Create all date combinations
        (SELECT * FROM generate_series('2023-01-01', '2023-01-10', INTERVAL '1 day') AS valid_date) AS d
),
-- Convert activity to bitmask representation
bits AS (
    SELECT 
        user_id,
        SUM(CASE
                WHEN is_active THEN POW(2, 9 - days_since)  -- Set bit for active days
                ELSE 0
            END
        )::BIGINT::BIT(10) AS datelist_int,  -- Convert to 10-bit binary
        ROW_NUMBER() OVER (PARTITION BY user_id)
    FROM daily_agg
    GROUP BY user_id
)
-- Final analysis using bitmask
SELECT
    user_id,
    datelist_int,
    BIT_COUNT(bits.datelist_int & CAST('1000000000' AS BIT(10))) > 0 AS last_day_active,  -- Check most recent day
    BIT_COUNT(bits.datelist_int & CAST('1111111000' AS BIT(10))) > 0 AS last_week_active  -- Check recent week
FROM bits














--###PART 5 ###-- Create hosts cumulative table
-- Creates a table to track host activity over time with:
-- host_name: The name of the host
-- host_activity_datelist: Array of dates the host was active
-- date: The date of the record
CREATE TABLE hosts_cumulated (
    host_name TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host_name)  -- One record per host
)












--###PART 6 ###-- Host activity tracking ETL process
-- Similar to part 3 but for host activity tracking:
-- 1. Deduplicates events
-- 2. Gets yesterday's host data
-- 3. Aggregates today's host activity
-- 4. Merges and updates the cumulative table
WITH events_aggregated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id, event_time) AS row_num
    FROM events
    WHERE user_id IS NOT NULL 
),
events_deduped AS (
    SELECT * FROM events_aggregated WHERE row_num = 1
),
today AS (
    SELECT * FROM events_deduped WHERE DATE(event_time) = DATE('2023-01-19')
),
yesterday AS (
    SELECT * FROM hosts_cumulated WHERE date = DATE('2023-01-18')
),
hosts_agg AS(
    SELECT host, DATE(event_time) AS date, COUNT(1) 
    FROM today 
    GROUP BY (host, DATE(event_time))
-- Insert or update host activity
INSERT INTO hosts_cumulated
SELECT 
    COALESCE(hg.host, y.host_name),
    CASE 
        WHEN y.host_activity_datelist IS NOT NULL
            THEN y.host_activity_datelist || ARRAY[DATE(hg.date)]  -- Append new date
        WHEN y.host_activity_datelist IS NULL
            THEN ARRAY[DATE(hg.date)]  -- Create new array
        ELSE ARRAY[]::DATE[]  -- Fallback empty array
    END AS host_activity_datelist,
    COALESCE(hg.date, y.date + INTERVAL '1 day') AS date
FROM hosts_agg hg FULL OUTER JOIN yesterday y ON hg.host = y.host_name
ON CONFLICT (host_name)
DO
    UPDATE
        SET host_activity_datelist = EXCLUDED.host_activity_datelist,
            date = EXCLUDED.date















--### PART 7 ###-- Create host activity reduced table
-- Creates a table for aggregated host metrics with:
-- month_start: First day of the month
-- host: Host identifier
-- hit_array: Array of hit counts
-- unique_visitors: Array of unique visitor counts
CREATE TABLE host_activity_reduced (
    month_start DATE,
    host TEXT,
    hit_array BIGINT[],
    unique_visitors NUMERIC[],
    PRIMARY KEY (host, month_start)  -- One record per host per month
)










--### PART 8 ###-- Host activity reduced ETL process
-- Aggregates host activity data at monthly level:
-- 1. Deduplicates events
-- 2. Aggregates today's activity
-- 3. Combines with yesterday's data
-- 4. Updates the reduced table
INSERT INTO host_activity_reduced
WITH events_aggregated AS (
    SELECT
        user_id,
        host,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY user_id, event_time) AS row_num
    FROM events
    WHERE user_id IS NOT NULL 
),
events_deduped AS (
    SELECT * FROM events_aggregated WHERE row_num = 1 AND DATE(event_time) = DATE('2023-01-03')
),
today AS (
    SELECT 
        user_id,
        host,
        DATE(event_time) AS date,
        COUNT(*) AS num_hits
    FROM events_deduped
    GROUP BY (user_id, host, DATE(event_time))
),
today_agg AS (
    SELECT
        host,
        ARRAY_AGG(user_id) AS user_list,
        ARRAY_AGG(num_hits) AS site_num_hits,
        date
    FROM today
    GROUP BY (host, date))
),
yesterday AS (
    SELECT * FROM host_activity_reduced
    WHERE month_start = DATE('2023-01-01'))
-- Insert or update monthly aggregates
SELECT 
    COALESCE(y.month_start, DATE_TRUNC('month', ta.date)) AS month_start,
    COALESCE(ta.host, y.host),
    CASE
        WHEN y.hit_array IS NOT NULL THEN y.hit_array || ta.site_num_hits
        WHEN y.hit_array IS NULL THEN ta.site_num_hits
        ELSE y.hit_array
    END AS hit_array,
    CASE
        WHEN y.unique_visitors IS NOT NULL THEN y.unique_visitors || ta.user_list
        WHEN y.unique_visitors IS NULL THEN ta.user_list
        ELSE y.unique_visitors
    END AS unique_visitors 
FROM today_agg ta FULL OUTER JOIN yesterday y
ON ta.host = y.host
ON CONFLICT (host, month_start)
DO
    UPDATE
        SET unique_visitors = EXCLUDED.unique_visitors,
            hit_array = EXCLUDED.hit_array
