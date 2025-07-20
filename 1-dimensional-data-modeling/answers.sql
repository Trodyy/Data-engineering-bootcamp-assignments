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
    WHERE current_year = 1969
),
today AS (
    SELECT
        actor,
        actorid,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
        AVG(rating) AS avg_rating  
    FROM actor_films
    WHERE year = 1970
    GROUP BY actorid, actor, year
    ORDER BY actorid
)

INSERT INTO actors
SELECT 
    COALESCE(y.actor, t.actor) AS actor,
    COALESCE(y.actorid, t.actorid) AS actorid,
	COALESCE(t.year, y.current_year + 1) AS current_year ,
    COALESCE(y.films, ARRAY[]::films[])
    || CASE WHEN t.films IS NOT NULL
        THEN t.films
        ELSE ARRAY[]::films[] END AS films,
    (CASE 
        WHEN t.avg_rating > 8 THEN 'star'
        WHEN t.avg_rating > 7 THEN 'good'
        WHEN t.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END)::quality_class AS quality_class,
    t.actorid IS NOT NULL AS is_active
    
FROM yesterday y
FULL OUTER JOIN today t
    ON t.actorid = y.actorid;










