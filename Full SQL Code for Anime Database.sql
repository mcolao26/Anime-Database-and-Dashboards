############################################################ Loading the Data and Creating Staging Table ##################################################################
Use anime;
SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

CREATE TABLE anilist_raw (
  id INT,
  idMal INT,
  title_romaji VARCHAR(150),
  title_english VARCHAR(150),
  title_userPreferred VARCHAR(150),
  type VARCHAR(50),
  format VARCHAR(50),
  status VARCHAR(50),
  description VARCHAR(1000),
  startDate_year VARCHAR(50),
  startDate_month VARCHAR(50),
  startDate_day VARCHAR(50),
  endDate_year VARCHAR(50),
  endDate_month VARCHAR(50),
  endDate_day VARCHAR(50),
  season VARCHAR(50),
  seasonYear VARCHAR(50),
  seasonInt VARCHAR(50),
  episodes VARCHAR(50),
  duration VARCHAR(50),
  chapters VARCHAR(50),
  volumes VARCHAR(50),
  contryOfOrigin VARCHAR(50),
  isLicensed VARCHAR(50),
  source VARCHAR(50),
  hashtag text,
  trailer_id text,
  trailer_site text,
  trailer_thumbnail BLOB,
  updatedAt INT,
  coverImage_extraLarge BLOB,
  coverImage_Large BLOB,
  coverImage_medium BLOB,
  coverImage_color BLOB,
  bannerImage BLOB,
  genres text,
  synonyms text,
  tags text,
  averageScore INT,
  meanScore INT,
  popularity INT,
  favourites INT,
  trending INT,
  rankings text,
  isFavourite VARCHAR(20),
  iaAdult VARCHAR(20),
  isLocked VARCHAR(20),
  siteURL VARCHAR(500),
  externalLinks text,
  streamingEpisodes text,
  relations text,
  characters text,
  staff text,
  studios text,
  nextAiringEpisode text,
  airingSchedule text,
  recommendations text,
  reviews text,
  stats_scoreDistribution TEXT,
  stats_statusDistribution TEXT
);


LOAD DATA LOCAL INFILE '/Users/mattc/Downloads/archive(2)/anilist_anime_data_complete.csv' #replace with your own path
INTO TABLE anilist_raw
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE anilist_staging #Done so I can perform preprocessing on the data without altering the orginal
SELECT * FROM anilist_raw;

SELECT * FROM anilist_staging;

################################################################ Data Preprocessing ######################################################################

/* Trimming Every Column */
UPDATE anilist_staging
SET
  title_romaji = TRIM(title_romaji),
  title_english = TRIM(title_english),
  title_userPreferred = TRIM(title_userPreferred),
  type = TRIM(type),
  format = TRIM(format),
  status = TRIM(status),
  description = TRIM(description),
  startDate_year = TRIM(startDate_year),
  startDate_month = TRIM(startDate_month),
  startDate_day = TRIM(startDate_day),
  endDate_year = TRIM(endDate_year),
  endDate_month = TRIM(endDate_month),
  endDate_day = TRIM(endDate_day),
  season = TRIM(season),
  seasonYear = TRIM(seasonYear),
  seasonInt = TRIM(seasonInt),
  episodes = TRIM(episodes),
  duration = TRIM(duration),
  chapters = TRIM(chapters),
  volumes = TRIM(volumes),
  contryOfOrigin = TRIM(contryOfOrigin),
  isLicensed = TRIM(isLicensed),
  source = TRIM(source),
  hashtag = TRIM(hashtag),
  trailer_id = TRIM(trailer_id),
  trailer_site = TRIM(trailer_site),
  genres = TRIM(genres),
  synonyms = TRIM(synonyms),
  tags = TRIM(tags),
  rankings = TRIM(rankings),
  isFavourite = TRIM(isFavourite),
  iaAdult = TRIM(iaAdult),
  isLocked = TRIM(isLocked),
  siteURL = TRIM(siteURL),
  externalLinks = TRIM(externalLinks),
  streamingEpisodes = TRIM(streamingEpisodes),
  relations = TRIM(relations),
  characters = TRIM(characters),
  staff = TRIM(staff),
  studios = TRIM(studios),
  nextAiringEpisode = TRIM(nextAiringEpisode),
  airingSchedule = TRIM(airingSchedule),
  recommendations = TRIM(recommendations),
  reviews = TRIM(reviews),
  stats_scoreDistribution = TRIM(stats_scoreDistribution),
  stats_statusDistribution = TRIM(stats_statusDistribution);





/* Removing Unecessary Columns */
SELECT id, idMal #Seeing if the ids match, just out of curiousity
FROM anilist_staging
where id != idMal;

Alter table anilist_staging #Dropping the MAL id since I never plan to integrate it with MAL
drop column idMal;

SELECT COUNT(title_english) #Seeing how many 'NUll' values there are
FROM anilist_staging
WHERE length(title_english) < 1;

ALTER TABLE anilist_staging #Since about 50% of the column is empty, I decided to remove it.
DROP COLUMN title_english;

SELECT id, title_romaji, title_UserPreferred #Seeing if any rows do not match titles. A portion fit this part but they are in unusual and unusable formatting.
FROM anilist_staging
WHERE title_romaji != title_UserPreferred;

SELECT id, title_romaji, title_UserPreferred #Seeing if any rows do match.
FROM anilist_staging
WHERE title_romaji = title_UserPreferred;

SELECT distinct(id)
FROM (SELECT id, title_romaji, title_UserPreferred #Seeing which ids I need to delete
FROM anilist_staging
WHERE title_romaji != title_UserPreferred) as tbl2;

DELETE FROM anilist_staging #Remove rows with those ids
where id = 0 OR id = 46;

ALTER TABLE anilist_staging #Dropped this column since it is now the exact same as title_romaji
DROP COLUMN title_UserPreferred;

SELECT DISTINCT type #Seeing if there are other types besides 'ANIME'
FROM anilist_staging;

ALTER TABLE anilist_staging #Dropped the column since it was all the same value and therefore served no purpose
DROP COLUMN type;

ALTER TABLE anilist_staging #Dropped the column since it serves no purpose
DROP COLUMN seasonInt;

SELECT DISTINCT contryOfOrigin, COUNT(*) #Seeing the distribution of anime country origin
FROM anilist_staging
GROUP BY contryOfOrigin;

ALTER TABLE anilist_staging #Drop these columns since they serve no purpose for this project
DROP COLUMN trailer_id,
DROP COLUMN trailer_site,
DROP COLUMN trailer_thumbnail;

SELECT DISTINCT chapters, COUNT(*) #Seeing the distribution of chapters
FROM anilist_staging
GROUP BY chapters;

SELECT DISTINCT volumes, COUNT(*) #Seeing the distribution of volumes
FROM anilist_staging
GROUP BY volumes;

ALTER TABLE anilist_staging #Drop these columns since they are empty
DROP COLUMN chapters,
DROP COLUMN volumes;

SELECT DISTINCT hashtag, COUNT(*) #Seeing the distribution of hashtags
FROM anilist_staging
GROUP BY hashtag;

ALTER TABLE anilist_staging #Dropping since it serves no purpose for this project
DROP COLUMN hashtag;

ALTER TABLE anilist_staging #Dropping since it serves no purpose for this project
DROP COLUMN updatedAt;

ALTER TABLE anilist_staging #Dropping since they will not be used for this project
DROP COLUMN coverImage_Large,
DROP COLUMN coverImage_medium,
DROP COLUMN coverImage_color,
DROP COLUMN bannerImage;

ALTER TABLE anilist_staging #Dropping since there is another column like this but this one has 0 for some anime
DROP COLUMN averageScore;

ALTER TABLE anilist_staging #Dropping these since they are extremely large, nested JSONs for the most part. Add nothing of value and will ultimately need very large tables with a TON of entries. Not useful fro this project
DROP COLUMN synonyms,
DROP COLUMN tags,
DROP COLUMN externalLinks,
DROP COLUMN relations,
DROP COLUMN characters,
DROP COLUMN staff,
DROP COLUMN studios,
DROP COLUMN recommendations;

SELECT distinct isFavourite, COUNT(*) #Pretty much all is FALSE and the ones that aren't are incoherent
FROM anilist_staging
GROUP BY isFavourite;

ALTER TABLE anilist_staging
DROP COLUMN isFavourite;

SELECT distinct isLocked, COUNT(*) #Same thing
FROM anilist_staging
GROUP BY isLocked;

ALTER TABLE anilist_staging
DROP COLUMN isLocked;

SELECT distinct iaAdult, COUNT(*) #Salvagable
FROM anilist_staging
GROUP BY iaAdult;

ALTER TABLE anilist_staging
DROP COLUMN reviews;

ALTER TABLE anilist_staging
DROP COLUMN rankings;

ALTER TABLE anilist_staging
DROP COLUMN streamingEpisodes,
DROP COLUMN nextAiringEpisode,
DROP COLUMN airingSchedule;



/* Standardizing the data */
SELECT DISTINCt format, COUNT(*) #Seeing the types of formats to explore and their counts
FROM anilist_staging
GROUP BY format;

SELECT * #Seeing if I should keep the two wihtout a format
FROM anilist_staging
WHERE format = "";

UPDATE anilist_staging #Updating format only since it is easy to look up
SET format = "MUSIC"
WHERE id = 103403 and title_romaji = "Yuki no Hi no Tayori";

UPDATE anilist_staging
SET format = "ONA"
WHERE id = 198703 and title_romaji = "Wu Sui Xinghe";

SELECT DISTINCT status, COUNT(*) #Doing the same for status
FROM anilist_staging
GROUP BY status;

SELECT * #Seeing if I should keep the one
FROM anilist_staging
WHERE status = "";

UPDATE anilist_staging #Updating status only since it is easy to look up
SET status = "RELEASING"
WHERE id = 198695 and title_romaji = "Kaiju Ditan Mai Dali";

SELECT DISTINCT startDate_year, COUNT(*) #Checking if there are is anything that shouldn't be there
FROM anilist_staging
GROUP BY startDate_year;

SELECT DISTINCT startDate_month, COUNT(*) #Checking if there are is anything that shouldn't be there
FROM anilist_staging
GROUP BY startDate_month;

SELECT DISTINCT startDate_day, COUNT(*) #Checking if there are is anything that shouldn't be there
FROM anilist_staging
GROUP BY startDate_day;

SELECT DISTINCT endDate_year, COUNT(*) #Checking if there are is anything that shouldn't be there
FROM anilist_staging
GROUP BY endDate_year;

SELECT DISTINCT endDate_month, COUNT(*) #Checking if there are is anything that shouldn't be there
FROM anilist_staging
GROUP BY endDate_month;

SELECT DISTINCT endDate_day, COUNT(*) #Checking if there are is anything that shouldn't be there
FROM anilist_staging
GROUP BY endDate_day;

SELECT DISTINCT season, COUNT(*) #Checking if there are is anything that shouldn't be there
FROM anilist_staging
GROUP BY season;

UPDATE anilist_staging
SET iaAdult = CASE
  WHEN iaAdult = 'TRUE'  THEN 'TRUE'
  WHEN iaAdult = 'FALSE' THEN 'FALSE'
  ELSE NULL
END;

DELETE FROM anilist_staging #provides us useless data so deleted
WHERE meanScore = 0
  AND popularity = 0;
  
SELECT *
FROM anilist_staging
WHERE siteURL NOT LIKE 'http%';

DELETE FROM anilist_staging
WHERE siteURL IS NOT NULL
  AND siteURL NOT LIKE 'http%';


/* Finding Duplicates */
WITH table_2 AS ( #Checking for duplicates using WITH and window functions. Found nothing so there is no need to remove anything.
SELECT 
id, 
title_romaji, 
format,
row_number() OVER(partition by id, title_romaji, format) as row_num
FROM anilist_staging
)
SELECT 
* 
FROM table_2
WHERE row_num > 1;

/* Changing blanks and [] to NULL */
UPDATE anilist_staging
SET
  title_romaji = NULLIF(title_romaji, ''),
  format = NULLIF(format, ''),
  status = NULLIF(status, ''),
  description = NULLIF(description, ''),
  startDate_year = NULLIF(startDate_year, ''),
  startDate_month = NULLIF(startDate_month, ''),
  startDate_day = NULLIF(startDate_day, ''),
  endDate_year = NULLIF(endDate_year, ''),
  endDate_month = NULLIF(endDate_month, ''),
  endDate_day = NULLIF(endDate_day, ''),
  season = NULLIF(season, ''),
  seasonYear = NULLIF(seasonYear, ''),
  episodes = NULLIF(episodes, ''),
  duration = NULLIF(duration, ''),
  contryOfOrigin = NULLIF(contryOfOrigin, ''),
  isLicensed = NULLIF(isLicensed, ''),
  source = NULLIF(source, ''),
  genres = NULLIF(genres, ''),
  synonyms = NULLIF(synonyms, ''),
  tags = NULLIF(tags, ''),
  rankings = NULLIF(rankings, ''),
  isFavourite = NULLIF(isFavourite, ''),
  iaAdult = NULLIF(iaAdult, ''),
  isLocked = NULLIF(isLocked, ''),
  siteURL = NULLIF(siteURL, ''),
  externalLinks = NULLIF(externalLinks, ''),
  streamingEpisodes = NULLIF(streamingEpisodes, ''),
  relations = NULLIF(relations, ''),
  characters = NULLIF(characters, ''),
  staff = NULLIF(staff, ''),
  studios = NULLIF(studios, ''),
  nextAiringEpisode = NULLIF(nextAiringEpisode, ''),
  airingSchedule = NULLIF(airingSchedule, ''),
  recommendations = NULLIF(recommendations, ''),
  reviews = NULLIF(reviews, ''),
  stats_scoreDistribution = NULLIF(stats_scoreDistribution, ''),
  stats_statusDistribution = NULLIF(stats_statusDistribution, '');
  
UPDATE anilist_staging
SET
  title_romaji = NULLIF(title_romaji, '[]'),
  format = NULLIF(format, '[]'),
  status = NULLIF(status, '[]'),
  description = NULLIF(description, '[]'),
  startDate_year = NULLIF(startDate_year, '[]'),
  startDate_month = NULLIF(startDate_month, '[]'),
  startDate_day = NULLIF(startDate_day, '[]'),
  endDate_year = NULLIF(endDate_year, '[]'),
  endDate_month = NULLIF(endDate_month, '[]'),
  endDate_day = NULLIF(endDate_day, '[]'),
  season = NULLIF(season, '[]'),
  seasonYear = NULLIF(seasonYear, '[]'),
  episodes = NULLIF(episodes, '[]'),
  duration = NULLIF(duration, '[]'),
  contryOfOrigin = NULLIF(contryOfOrigin, '[]'),
  isLicensed = NULLIF(isLicensed, '[]'),
  source = NULLIF(source, '[]'),
  genres = NULLIF(genres, '[]'),
  synonyms = NULLIF(synonyms, '[]'),
  tags = NULLIF(tags, '[]'),
  rankings = NULLIF(rankings, '[]'),
  isFavourite = NULLIF(isFavourite, '[]'),
  iaAdult = NULLIF(iaAdult, '[]'),
  isLocked = NULLIF(isLocked, '[]'),
  siteURL = NULLIF(siteURL, '[]'),
  externalLinks = NULLIF(externalLinks, '[]'),
  streamingEpisodes = NULLIF(streamingEpisodes, '[]'),
  relations = NULLIF(relations, '[]'),
  characters = NULLIF(characters, '[]'),
  staff = NULLIF(staff, '[]'),
  studios = NULLIF(studios, '[]'),
  nextAiringEpisode = NULLIF(nextAiringEpisode, '[]'),
  airingSchedule = NULLIF(airingSchedule, '[]'),
  recommendations = NULLIF(recommendations, '[]'),
  reviews = NULLIF(reviews, '[]'),
  stats_scoreDistribution = NULLIF(stats_scoreDistribution, '[]'),
  stats_statusDistribution = NULLIF(stats_statusDistribution, '[]');

/* Changing Nested Fields */
#The following created, cleans, and populated a genre table by parsing JSON values
CREATE TABLE staging_genres (
anime_id INT,
tyggenre VARCHAR(50)
);

INSERT INTO staging_genres (anime_id, genre)
SELECT
a.id,
jt.genre
FROM anilist_staging a
JOIN JSON_TABLE(a.genres,'$[*]' COLUMNS (genre VARCHAR(50) PATH '$')) jt
WHERE a.genres IS NOT NULL;

SELECT genre, COUNT(*)
FROM staging_genres
GROUP BY genre
ORDER BY COUNT(*) DESC;

UPDATE staging_genres
SET genre = TRIM(genre);

CREATE TABLE genre (
genre_id INT AUTO_INCREMENT PRIMARY KEY,
genre_name VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO genre (genre_name)
SELECT DISTINCT genre
FROM staging_genres;

SELECT * FROM genre ORDER BY genre_name;




/* Creating the Database and Populating Tables */
CREATE TABLE anime ( #Everything, except genres is kept in one table since it is better for Tableau and will make that process faster and more efficient
anime_id INT NOT NULL,
title VARCHAR(150) NOT NULL,
format VARCHAR(10),
status VARCHAR(50),
source VARCHAR(50),
season VARCHAR(10),
season_year INT,
start_date DATE,
end_date DATE,
episodes INT,
duration_minutes INT,
country_of_origin CHAR(2),
is_licensed BOOLEAN,
is_adult BOOLEAN,
mean_score INT,
popularity INT,
favourites INT,
trending INT,
site_url VARCHAR(500),
PRIMARY KEY (anime_id) 
);

INSERT INTO anime 
SELECT
s.id AS anime_id,
s.title_romaji AS title,
s.format,
s.status,
s.source,
s.season,
CAST(s.seasonYear AS UNSIGNED),
STR_TO_DATE(CONCAT(s.startDate_year, '-', s.startDate_month, '-', s.startDate_day), '%Y-%m-%d'),
STR_TO_DATE(CONCAT(s.endDate_year, '-', s.endDate_month, '-', s.endDate_day), '%Y-%m-%d'),
CAST(s.episodes AS UNSIGNED),
CAST(s.duration AS UNSIGNED),
s.contryOfOrigin,
CASE
	WHEN s.isLicensed = 'TRUE' THEN 1
	WHEN s.isLicensed = 'FALSE' THEN 0
    ELSE NULL
END,
CASE
	WHEN s.iaAdult = 'TRUE' THEN 1
	WHEN s.iaAdult = 'FALSE' THEN 0
    ELSE NULL
END,
s.meanScore,
s.popularity,
s.favourites,
s.trending,
s.siteURL
FROM anilist_staging s
WHERE s.id IS NOT NULL;


CREATE TABLE anime_genre (
anime_id INT NOT NULL,
genre_id INT NOT NULL,
PRIMARY KEY (anime_id, genre_id),
FOREIGN KEY (anime_id) REFERENCES anime(anime_id) ON DELETE CASCADE,
FOREIGN KEY (genre_id) REFERENCES genre(genre_id) ON DELETE CASCADE
);

INSERT INTO anime_genre (anime_id, genre_id)
SELECT
sg.anime_id,
g.genre_id
FROM staging_genres sg
JOIN anime a ON sg.anime_id = a.anime_id
JOIN genre g ON sg.genre = g.genre_name;

DROP TABLE staging_genres;





/* For Tableau */
ALTER TABLE anime
ADD COLUMN cover_image_url VARCHAR(500);

UPDATE anime a
JOIN anilist_staging s
  ON a.anime_id = s.id
SET a.cover_image_url = s.coverImage_extraLarge
WHERE s.coverImage_extraLarge IS NOT NULL;

ALTER TABLE anime
ADD COLUMN cover_image_medium VARCHAR(500);

UPDATE anime a #Thought I might use the photos but it turned out to be too much for Tableau to handle :(
JOIN anilist_raw r
  ON a.anime_id = r.id
SET a.cover_image_medium = r.coverImage_medium
WHERE r.coverImage_medium IS NOT NULL
  AND r.coverImage_medium <> '';
  
CREATE INDEX idx_anime_season_year ON anime (season_year);
CREATE INDEX idx_anime_format ON anime (format);
CREATE INDEX idx_anime_status ON anime (status);
CREATE INDEX idx_anime_is_adult ON anime (is_adult);
CREATE INDEX idx_anime_cover_medium ON anime (cover_image_medium(50));
CREATE INDEX idx_anime_cover_xl ON anime (cover_image_url(50));
CREATE INDEX idx_anime_genre_anime ON anime_genre(anime_id); #indexes for Tableau to perform better
CREATE INDEX idx_anime_genre_genre ON anime_genre(genre_id);
CREATE INDEX idx_genre_name ON genre(genre_name);

