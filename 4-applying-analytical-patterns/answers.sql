### PART 1 ###--


--First of all I've to create players table again.
--To create players seasons we need to have 2 types by names season_stats and scoring class
 CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );
 CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');

--Creating players table.
 CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );
--Inserting data into players table till season = 2015
 WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 2014

), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 2015
)
INSERT INTO players
SELECT
        COALESCE(ls.player_name, ts.player_name) as player_name,
        COALESCE(ls.height, ts.height) as height,
        COALESCE(ls.college, ts.college) as college,
        COALESCE(ls.country, ts.country) as country,
        COALESCE(ls.draft_year, ts.draft_year) as draft_year,
        COALESCE(ls.draft_round, ts.draft_round) as draft_round,
        COALESCE(ls.draft_number, ts.draft_number)
            as draft_number,
        COALESCE(ls.seasons,
            ARRAY[]::season_stats[]
            ) || CASE WHEN ts.season IS NOT NULL THEN
                ARRAY[ROW(
                ts.season,
                ts.pts,
                ts.ast,
                ts.reb, ts.weight)::season_stats]
                ELSE ARRAY[]::season_stats[] END
            as seasons,
         CASE
             WHEN ts.season IS NOT NULL THEN
                 (CASE WHEN ts.pts > 20 THEN 'star'
                    WHEN ts.pts > 15 THEN 'good'
                    WHEN ts.pts > 10 THEN 'average'
                    ELSE 'bad' END)::scoring_class
             ELSE ls.scoring_class
         END as scoring_class,
         ts.season IS NOT NULL as is_active,
         2015 AS current_season

    FROM last_season ls
    FULL OUTER JOIN this_season ts
    ON ls.player_name = ts.player_name





--Now , Like as done before in the lab I created a table to trach players activity.
CREATE TABLE players_growth (
	player_name TEXT ,
	first_active_season INTEGER ,
	last_active_season INTEGER ,
	activity_state TEXT ,
	seasons_active INTEGER[] ,
	season INTEGER ,
	PRIMARY KEY (player_name , season)
)



-- Inserting data
INSERT INTO players_growth
WITH yesterday AS (      --I defined CTE yesterday from players_growth tables
	SELECT * FROM players_growth
	WHERE season = 2005
) ,
	agg_players AS (       --Before defining today, I aggergate players to prevent from repeated rows.
		SELECT
 			player_name ,
			seasons ,
			MIN(current_season) AS current_season   --We assume the minimum of the current_season
		FROM players
		GROUP BY (player_name , seasons)
		ORDER BY player_name , seasons
	) ,
	today AS (
		SELECT * FROM agg_players       --Now , Lets define today CTE
		WHERE current_season =2006      
	)

SELECT
	COALESCE(t.player_name , y.player_name) AS player_name ,
	COALESCE(y.first_active_season , t.current_season) AS first_active_season ,
	COALESCE(t.current_season , y.last_active_season) AS last_active_season ,
	CASE
		WHEN y.player_name IS NULL THEN 'New'    --If There is no activity before , He assume as NEW
		WHEN y.last_active_season = t.current_season - 1 THEN 'Continued Playing'    -- If  the distance between this year and the last activity year is 1 then it means he is continueing.
		WHEN y.last_active_season < t.current_season - 1 THEN 'Returned from Retirement'   --If the distance was smaller it means he came back from a resurrected time
		WHEN t.player_name IS NULL AND y.last_active_season = y.season THEN 'Retired' -- It's plain		
		ELSE 'Stayed Retired'
	END AS activity_state ,
	COALESCE(y.seasons_active ,
		ARRAY[]::INTEGER[]) || CASE
		WHEN t.player_name IS NOT NULL THEN ARRAY[t.current_season]
			ELSE ARRAY[]::INTEGER[]
		END AS season_active ,
	 COALESCE(t.current_season, y.season + 1 ) as season -- Add dates active
FROM today t
FULL OUTER JOIN yesterday y   --Use FULL OUTER JOIN
ON t.player_name = y.player_name





--### PART 2 ###--
WITH shirin AS (
	SELECT 
	COALESCE(gd.player_id , 0) AS player_id ,
	COALESCE(gd.team_id , 0) AS team_id ,
	COALESCE(g.season , 0) AS season ,
	COALESCE(SUM(pts) , 0) AS sum_points
FROM game_details gd
JOIN games g
ON gd.game_id = g.game_id
GROUP BY GROUPING SETS(
	(player_id , team_id) ,
	(player_id , season) ,
	(team_id) ,
	(season)
)
)

SELECT * FROM shirin
WHERE team_id = 0
ORDER BY sum_points DESC
