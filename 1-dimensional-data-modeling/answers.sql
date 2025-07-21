--###PART 1###--
---DDL for actors table : Create a DDL for an actors table with the following fields 

-- Create custom types first
CREATE TYPE films AS (
    film TEXT , 
    votes INTEGER , 
    rating REAL ,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- Create actors table with proper primary key
CREATE TABLE actors (
    actorid TEXT PRIMARY KEY , 
    actor TEXT NOT NULL,
    current_year INTEGER NOT NULL ,
    films films[] ,
    quality_class quality_class ,
    is_active BOOLEAN ,
);

COMMENT ON TABLE actors IS 'Master table of actors with their current state';
COMMENT ON COLUMN actors.quality_class IS 'Performance quality based on most recent year''s average rating';








--###PART 2###--
--Cumulative table generation query: Write a query that populates the actors table one year at a time.


W-- Cumulative load for year 1979 with better documentation
WITH 
-- Get previous state (1978 data)
previous_year AS (
    SELECT * FROM actors 
    WHERE current_year = 1978
),

-- Aggregate current year films by actor
current_year_films AS (
    SELECT
        actorid ,
        actor ,
        year ,
        ARRAY_AGG(ROW(film , votes , rating , filmid)::films) AS films ,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 1979
    GROUP BY actorid , actor , year
),

-- Combine previous and current data
combined_data AS (
    SELECT 
        COALESCE(p.actorid, c.actorid) AS actorid ,
        COALESCE(p.actor, c.actor) AS actor ,
        1979 AS current_year ,
        CASE 
            WHEN c.actorid IS NOT NULL THEN 
                COALESCE(p.films , ARRAY[]::films[]) || c.films
            ELSE p.films
        END AS films,
        
        CASE
            WHEN c.actorid IS NOT NULL THEN
                CASE 
                    WHEN c.avg_rating > 8 THEN 'star'::quality_class
                    WHEN c.avg_rating > 7 THEN 'good'::quality_class
                    WHEN c.avg_rating > 6 THEN 'average'::quality_class
                    ELSE 'bad'::quality_class
                END
            ELSE p.quality_class
        END AS quality_class,
        
        c.actorid IS NOT NULL AS is_active
    FROM previous_year p
    FULL OUTER JOIN current_year_films c
        ON p.actorid = c.actorid
)

-- Upsert operation with conflict handling
INSERT INTO actors
SELECT * FROM combined_data
ON CONFLICT (actorid) DO UPDATE
SET
    actor = EXCLUDED.actor ,
    current_year = EXCLUDED.current_year ,
    films = EXCLUDED.films ,
    quality_class = EXCLUDED.quality_class ,
    is_active = EXCLUDED.is_active ;











--###PART 3###--
--DDL for actors_history_scd table: Create a DDL for an actors_history_scd table.
CREATE TABLE actors_history_scd (
    actor_history_id SERIAL PRIMARY KEY ,
    actorid TEXT NOT NULL ,
    actor TEXT NOT NULL, 
    quality_class quality_class NOT NULL ,
    is_active BOOLEAN NOT NULL ,
    start_year INTEGER NOT NULL ,
    end_year INTEGER ,
    current_year INTEGER NOT NULL
);

COMMENT ON TABLE actors_history_scd IS 'Type 2 SCD table tracking actor quality class and activity changes over time';
COMMENT ON COLUMN actors_history_scd.end_year IS 'NULL indicates current active record';







--###PART 4###--
--Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

WITH actor_years AS (
    SELECT 
        actorid,
        actor,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class , 1) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_quality,
        LAG(is_active , 1) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_active
    FROM actors
    ORDER BY actorid, current_year
),

change_points AS (
    SELECT
        actorid,
        actor,
        current_year,
        quality_class,
        is_active,
        CASE 
            WHEN quality_class <> prev_quality OR is_active <> prev_active 
                 OR prev_quality IS NULL THEN 1 
            ELSE 0 
        END AS status_changed
    FROM actor_years
),

streaks AS (
    SELECT
        actorid,
        actor,
        current_year,
        quality_class,
        is_active,
        SUM(status_changed) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_id
    FROM change_points
)

INSERT INTO actors_history_scd
    (actorid, actor, quality_class, is_active, start_year, end_year, current_year)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year,
    MAX(current_year) AS current_year
FROM streaks
GROUP BY actorid, actor, quality_class, is_active, streak_id
ORDER BY actorid, start_year;










--###PART 5###--
--Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.
CREATE TYPE actors_scd AS (
	quality_class quality_class ,
	is_active BOOLEAN ,
	start_year INTEGER ,
	end_year INTEGER
)
WITH last_year_scd AS (
	SELECT actor , quality_class , is_active , start_year , end_year FROM actors_history_scd
	WHERE end_year = 1978 AND current_year = 1978
) ,
	historical_scd AS (
		SELECT actor , quality_class , is_active , start_year , end_year FROM actors_history_scd
	WHERE end_year < 1978 AND current_year= 1978
) ,
	this_year_data AS (
		SELECT * FROM actors
		WHERE current_year = 1979
	) ,
	unchanged_records AS (
		SELECT
			ly.actor AS actor ,
			ly.quality_class ,
			ly.is_active ,
			ly.start_year ,
			ty.current_year AS end_year
		FROM this_year_data ty
		JOIN last_year_scd ly
		ON ty.actor = ly.actor
		WHERE ty.quality_class = ly.quality_class
		AND ty.is_active = ly.is_active
	) ,
	changed_records AS (
		SELECT 
			ty.actor as actor ,
			UNNEST(
				ARRAY[ROW(
						ly.quality_class ,
						ly.is_active ,
						ly.start_year ,
						ly.end_year
					)::actors_scd ,
					  ROW(
					  	ty.quality_class ,
						ty.is_active ,
						ty.current_year ,
						ty.current_year
				)::actors_scd]
			) AS records
		FROM this_year_data ty
		LEFT JOIN last_year_scd ly
		ON ty.actor = ly.actor
		WHERE ty.quality_class <> ly.quality_class
		OR ty.is_active <> ly.is_active
	) ,
	unnested_changed_records AS (
		SELECT
			actor as actor ,
			(records::actors_scd).quality_class ,
            (records::actors_scd).is_active,
            (records::actors_scd).start_year,
            (records::actors_scd).end_year
		FROM changed_records
	) ,
	new_records AS (
		SELECT
			ty.actor ,
			ty.quality_class ,
			ty.is_active ,
			ty.current_year AS start_year ,
			ty.current_year AS end_year
		FROM this_year_data ty
		LEFT JOIN last_year_scd ly
		ON ty.actor = ly.actor
		WHERE ly.actor IS NULL
	)

SELECT *, 1979 AS current_year FROM (
    SELECT actor, quality_class, is_active, start_year, end_year FROM historical_scd
UNION ALL
SELECT actor, quality_class, is_active, start_year, end_year FROM unchanged_records
UNION ALL
SELECT actor, quality_class, is_active, start_year, end_year FROM unnested_changed_records
UNION ALL
SELECT actor, quality_class, is_active, start_year, end_year FROM new_records
) total

	






