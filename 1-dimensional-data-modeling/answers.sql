--###PART 1###--
---DDL for actors table : Create a DDL for an actors table with the following fields 
CREATE TYPE films AS (
	film TEXT , 
	votes INTEGER , 
	rating REAL ,
	filmid TEXT
)

CREATE TYPE quality_class AS ENUM ('star' , 'good' , 'average' , 'bad')

CREATE TABLE actors (
	actorid TEXT , 
	actor TEXT ,
	current_year INTEGER ,
	films films[] ,
	quality_class quality_class ,
	is_active BOOLEAN ,
	PRIMARY KEY(actorid , actor)
	);








--###PART 2###--
--Cumulative table generation query: Write a query that populates the actors table one year at a time.


WITH yesterday AS (
    SELECT * FROM actors 
    WHERE current_year = 1978
),

today_aggregated AS (
    SELECT
        actor ,
		actorid ,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 1979
    GROUP BY actorid, actor, year
),
combined AS (
    SELECT 
        COALESCE(y.actor, t.actor) AS actor,
		COALESCE(y.actorid, t.actorid) AS actorid,
        COALESCE(t.year, y.current_year + 1) AS current_year,
        COALESCE(y.films, ARRAY[]::films[]) || 
        COALESCE(t.films, ARRAY[]::films[]) AS films,
        CASE
            WHEN t.actorid IS NOT NULL THEN
                CASE 
                    WHEN t.avg_rating > 8 THEN 'star'::quality_class
                    WHEN t.avg_rating > 7 THEN 'good'::quality_class
                    WHEN t.avg_rating > 6 THEN 'average'::quality_class
                    ELSE 'bad'::quality_class
                END
            ELSE y.quality_class
        END AS quality_class,
        t.actorid IS NOT NULL AS is_active
    FROM yesterday y
    FULL OUTER JOIN today_aggregated t
        ON t.actorid = y.actorid
)



INSERT INTO actors
SELECT * FROM combined
ON CONFLICT (actorid, actor) DO UPDATE
SET
    films = EXCLUDED.films,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active,
    current_year = EXCLUDED.current_year;


--I inerto into year = 1979(I mean assume this year is 1979)











--###PART 3###--
--DDL for actors_history_scd table: Create a DDL for an actors_history_scd table.

CREATE TABLE actors_scd_type (
	actor TEXT ,
	quality_class quality_class ,
	is_active BOOLEAN ,
	start_season INTEGER , 
	end_season INTEGER ,
	current_season INTEGER
)








--###PART 4###--
--Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.




WITH streak_started AS (
    SELECT actor,
           current_year,
           quality_class,
           LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) <> quality_class
               OR LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) IS NULL
               AS did_change
    FROM actors
),
     streak_identified AS (
         SELECT
           actor,
                quality_class,
                current_year,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            actor,
            quality_class,
            streak_identifier,
            MIN(current_year) AS start_date,
            MAX(current_year) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3
     )

     SELECT actor, quality_class, start_date, end_date
     FROM aggregated












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





	






