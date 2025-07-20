--###PART 1###--

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

-- Upsert operation
INSERT INTO actors
SELECT * FROM combined
ON CONFLICT (actorid, actor) DO UPDATE
SET
    films = EXCLUDED.films,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active,
    current_year = EXCLUDED.current_year;



