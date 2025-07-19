-- CREATE TYPE films AS (
-- 	film TEXT , 
-- 	votes INTEGER , 
-- 	rating REAL ,
-- 	filmid TEXT
-- )

-- CREATE TYPE quality_class AS ENUM ('star' , 'good' , 'average' , 'bad')

-- CREATE TABLE actors (
-- 	actorid TEXT , 
-- 	actor TEXT ,
-- 	films films[] ,
-- 	quality_class quality_class ,
-- 	is_active BOOLEAN ,
-- 	PRIMARY KEY(actorid)
-- 	);

-- SELECT * FROM public.actor_films
-- ORDER BY actorid ASC, filmid ASC 




-- This inserts data for the year 1970 into the actors table

WITH
-- Get previous state of actors (e.g., after 1969)
yesterday AS (
  SELECT *
  FROM actors
),

-- New data for 1970
today AS (
  SELECT *
  FROM actor_films
  WHERE year = 1970
),

-- Combine previous data with new films
combined AS (
  SELECT
    COALESCE(y.actorid, t.actorid) AS actorid,
    COALESCE(y.actor, t.actor) AS actor,
    
    -- Combine existing films array with new film
    COALESCE(y.films, ARRAY[]::films[])
	|| CASE WHEN t.filmid IS NOT NULL THEN
     ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::films]
   ELSE ARRAY[]::films[] END AS films ,

    
    -- Calculate quality_class based on average rating of this year's films
    (CASE 
      WHEN AVG(t.rating) OVER (PARTITION BY t.actorid) > 8 THEN 'star'
      WHEN AVG(t.rating) OVER (PARTITION BY t.actorid) > 7 THEN 'good'
      WHEN AVG(t.rating) OVER (PARTITION BY t.actorid) > 6 THEN 'average'
      ELSE 'bad'
    END)::quality_class AS quality_class,

    -- Set is_active true if they had a film this year
    CASE WHEN t.film IS NOT NULL THEN TRUE ELSE FALSE END AS is_active

  FROM yesterday y
  FULL OUTER JOIN today t
    ON y.actorid = t.actorid
)

-- Insert into actors table
INSERT INTO actors (actorid, actor, films, quality_class, is_active)
SELECT *
FROM combined
ON CONFLICT (actorid) DO UPDATE
SET 
    actor = EXCLUDED.actor,
    films = EXCLUDED.films,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active;





