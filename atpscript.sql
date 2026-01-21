-- atpscript.sql

-------------------------
-- Criação das Tabelas --
-------------------------

CREATE DATABASE IF NOT EXISTS atp;
USE atp;

DROP TABLE IF EXISTS match_info;
DROP TABLE IF EXISTS player_info;
DROP TABLE IF EXISTS tournaments;
DROP TABLE IF EXISTS countries;

CREATE TABLE countries (
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE player_info (
    player_id INT AUTO_INCREMENT PRIMARY KEY,
    hand VARCHAR(10),
    born_country VARCHAR(255),
    player_name VARCHAR(100) NOT NULL UNIQUE,
    FOREIGN KEY (born_country) REFERENCES countries(country_name)
        ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE tournaments (
    tournament_id INT AUTO_INCREMENT PRIMARY KEY,
    location VARCHAR(255),
    tournament_name VARCHAR(255) NOT NULL,
    start_date DATE NOT NULL,
    FOREIGN KEY (location) REFERENCES countries(country_name)
        ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE match_info (
    game_id INT AUTO_INCREMENT PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    ground VARCHAR(10),
    oponent VARCHAR(255),
    wl VARCHAR(10),
    match_id VARCHAR(255) NOT NULL,
    tournament_name VARCHAR(255),
    FOREIGN KEY (player_name) REFERENCES player_info(player_name)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX idx_tournament_name ON tournaments(tournament_name);
CREATE INDEX idx_start_date ON tournaments(start_date);

---------------------------
-- Importação para MySQL -- ===> Desativar FK check
---------------------------

LOAD DATA INFILE '/var/lib/mysql-files/atp.countries.csv'
IGNORE INTO TABLE countries 
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@dummy, country_name);

LOAD DATA INFILE '/var/lib/mysql-files/atp.player_info.csv'
IGNORE INTO TABLE player_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@dummy, hand, born_country, player_name);
 
LOAD DATA INFILE '/var/lib/mysql-files/atp.tournaments.csv'
INTO TABLE tournaments
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@dummy, location, tournament_name, @start_date)
SET start_date = STR_TO_DATE(@start_date, '%Y-%m-%dT%H:%i:%s.000Z');

LOAD DATA INFILE '/var/lib/mysql-files/atp.game_info.csv'
INTO TABLE match_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(@dummy, player_name, ground, oponent, wl, tournament_name, match_id);


---------------------
-- Criação de FK's -- ==> Desativar FK check
---------------------


ALTER TABLE match_info ADD COLUMN tournament_id INT;
ALTER TABLE match_info ADD COLUMN temp_match_date DATE;

----------

CREATE INDEX idx_match_tourn_name ON match_info(tournament_name);
CREATE INDEX idx_match_temp_date ON match_info(temp_match_date);

----------

UPDATE match_info
SET temp_match_date = DATE(LEFT(SUBSTRING_INDEX(SUBSTRING_INDEX(match_id, '|', 3), '|', -1), 10));

----------

UPDATE match_info m
JOIN tournaments t 
    ON m.tournament_name = t.tournament_name 
    AND m.temp_match_date = t.start_date
SET m.tournament_id = t.tournament_id;

----------

ALTER TABLE match_info
ADD CONSTRAINT fk_match_tournament
FOREIGN KEY (tournament_id) 
REFERENCES tournaments(tournament_id)
ON DELETE RESTRICT 
ON UPDATE CASCADE;

----------

ALTER TABLE match_info DROP COLUMN tournament_name;
ALTER TABLE match_info DROP COLUMN temp_match_date;

--------------
-- QUESTÕES --
--------------

-- Q.1

SELECT 
    c.country_name AS Pais,
    COALESCE(stats_players.total_jogadores, 0) AS Total_Jogadores,
    COALESCE(stats_matches.total_torneios, 0) AS Total_Torneios_Unicos,
    COALESCE(stats_matches.total_jogos, 0) AS Total_Jogos
FROM countries c

LEFT JOIN (
    SELECT born_country, COUNT(*) AS total_jogadores
    FROM player_info
    GROUP BY born_country
) stats_players ON c.country_name = stats_players.born_country


LEFT JOIN (
    SELECT 
        t.location, 
        COUNT(DISTINCT t.tournament_name) AS total_torneios,
        COUNT(DISTINCT m.match_id) AS total_jogos
    FROM tournaments t
    LEFT JOIN match_info m ON t.tournament_id = m.tournament_id
    GROUP BY t.location
) stats_matches ON c.country_name = stats_matches.location

ORDER BY Total_Torneios_Unicos DESC
LIMIT 25;

-- Q2

SELECT
    T1.player_name,
    ROUND(CAST(SUM(CASE WHEN T2.wl = 'W' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T2.game_id), 3) AS Winning_Percentage
FROM
    player_info AS T1
INNER JOIN
    match_info AS T2 ON T1.player_name = T2.player_name
GROUP BY
    T1.player_name
HAVING
    COUNT(T2.game_id) > 30
ORDER BY
    Winning_Percentage DESC
LIMIT 10;

-- Q3

SELECT
    p.player_name,
    ROUND(CAST(SUM(CASE WHEN m.wl = 'W' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(m.game_id), 3) AS Grand_Slam_Winning_Percentage
FROM
    player_info AS p
INNER JOIN
    match_info AS m ON p.player_name = m.player_name
INNER JOIN
    tournaments AS t ON m.tournament_id = t.tournament_id
WHERE
    p.hand = 'Left' 
    AND (
        t.tournament_name LIKE '%Australian Open%'
        OR t.tournament_name LIKE '%Roland Garros%'
        OR t.tournament_name LIKE '%Wimbledon%'
        OR t.tournament_name LIKE '%US Open%'
    )
GROUP BY
    p.player_name
HAVING
    COUNT(m.game_id) > 30
ORDER BY
    Grand_Slam_Winning_Percentage DESC
LIMIT 10;

-- Q4

SELECT
    T1.player_name,
    SUM(CASE WHEN T2.wl = 'W' THEN 1 ELSE 0 END) AS Hard_Ground_Wins
FROM
    player_info AS T1
INNER JOIN
    match_info AS T2 ON T1.player_name = T2.player_name
WHERE
    T2.ground = 'Hard'
GROUP BY
    T1.player_name
HAVING
    COUNT(T2.game_id) > 30
ORDER BY
    Hard_Ground_Wins DESC
LIMIT 5;