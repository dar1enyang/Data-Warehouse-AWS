import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# SCHEMA
nodist_schema_create = "CREATE SCHEMA IF NOT EXISTS nodist;"
dist_schema_create   = "CREATE SCHEMA IF NOT EXISTS dist;"

nodist_schema_set    = "SET search_path TO nodist;"
dist_schema_set      = "SET search_path TO dist;"
# DROP TABLES
## Staging tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs_table"

## Fact & Dimension tables
songplay_table_drop       = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop           = "DROP TABLE IF EXISTS user_table;"
song_table_drop           = "DROP TABLE IF EXISTS song_table;"
artist_table_drop         = "DROP TABLE IF EXISTS artist_table;"
time_table_drop           = "DROP TABLE IF EXISTS time_table;"

# CREATE NO DIST TABLES
no_dist_staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events_table (
                                  artist TEXT, 
                                  auth VARCHAR(30), 
                                  firstName TEXT, 
                                  gender VARCHAR(5), 
                                  itemInSession SMALLINT,
                                  lastName TEXT, 
                                  length FLOAT, 
                                  level VARCHAR(10), 
                                  location TEXT,
                                  method VARCHAR(10),
                                  page VARCHAR(20),
                                  registration BIGINT, 
                                  sessionId INT,
                                  song TEXT,
                                  status SMALLINT,
                                  ts BIGINT,
                                  userAgent TEXT,
                                  userId INT)
                                  ;"""
                             )

no_dist_staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table
                                 (num_songs SMALLINT,
                                  artist_id TEXT,
                                  artist_latitude FLOAT,
                                  artist_longitude FLOAT,
                                  artist_location TEXT,
                                  artist_name TEXT,
                                  song_id TEXT,
                                  title TEXT,
                                  duration FLOAT,
                                  year SMALLINT)
                                  ;"""
                             )

no_dist_songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table
                            (songplay_id INT IDENTITY(0,1),
                             start_time BIGINT NOT NULL,
                             user_id INT NOT NULL,
                             level VARCHAR(10),
                             song_id TEXT NOT NULL,
                             artist_id TEXT NOT NULL,
                             session_id INT,
                             location TEXT,
                             user_agent TEXT,
                             PRIMARY KEY(songplay_id))
                             ;"""
                        )

no_dist_user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
                        (user_id INT,
                         first_name TEXT, 
                         last_name TEXT,
                         gender VARCHAR(5),
                         level VARCHAR(10),
                         PRIMARY KEY(user_id))
                         ;"""
                    )

no_dist_song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table
                        (song_id TEXT,
                         title TEXT,
                         artist_id TEXT NOT NULL,
                         year SMALLINT,
                         PRIMARY KEY(song_id),
                         duration FLOAT)
                         ;"""
                    )

no_dist_artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table
                          (artist_id TEXT,
                           name TEXT,
                           location TEXT,
                           lattitude FLOAT,
                           longitude FLOAT,
                           PRIMARY KEY(artist_id))
                           ;"""
                      )

no_dist_time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table
                        (start_time BIGINT,
                         hour SMALLINT,
                         day SMALLINT,
                         week SMALLINT,
                         month SMALLINT,
                         year SMALLINT,
                         weekday SMALLINT,
                         PRIMARY KEY(start_time))
                         ;"""
                    )

# CREATE DIST TABLES
staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events_table (
                                  artist TEXT, 
                                  auth VARCHAR(30), 
                                  firstName TEXT, 
                                  gender VARCHAR(5), 
                                  itemInSession SMALLINT,
                                  lastName TEXT, 
                                  length FLOAT, 
                                  level VARCHAR(10), 
                                  location TEXT,
                                  method VARCHAR(10),
                                  page VARCHAR(20) sortkey,
                                  registration BIGINT, 
                                  sessionId INT,
                                  song TEXT,
                                  status SMALLINT,
                                  ts BIGINT,
                                  userAgent TEXT,
                                  userId INT distkey)
                                  ;"""
                             )

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table
                                 (num_songs SMALLINT,
                                  artist_id TEXT,
                                  artist_latitude FLOAT,
                                  artist_longitude FLOAT,
                                  artist_location TEXT,
                                  artist_name TEXT,
                                  song_id TEXT distkey,
                                  title TEXT,
                                  duration FLOAT,
                                  year SMALLINT)
                                  ;"""
                             )

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table
                            (songplay_id INT IDENTITY(0,1),
                             start_time BIGINT NOT NULL sortkey,
                             user_id INT NOT NULL,
                             level VARCHAR(10),
                             song_id TEXT NOT NULL distkey,
                             artist_id TEXT NOT NULL,
                             session_id INT,
                             location TEXT,
                             user_agent TEXT,
                             PRIMARY KEY(songplay_id))
                             ;"""
                             )

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
                        (user_id INT ,
                         first_name TEXT, 
                         last_name TEXT,
                         gender VARCHAR(5) distkey,
                         level VARCHAR(10) sortkey,
                         PRIMARY KEY(user_id))
                         ;"""
                    )

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table
                        (song_id TEXT sortkey distkey,
                         title TEXT,
                         artist_id TEXT NOT NULL,
                         year SMALLINT,
                         duration FLOAT,
                         PRIMARY KEY(song_id))
                         ;"""
                    )

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table
                          (artist_id TEXT sortkey distkey,
                           name TEXT,
                           location TEXT,
                           lattitude FLOAT,
                           longitude FLOAT,
                           PRIMARY KEY(artist_id))
                           ;"""
                      )

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table
                        (start_time BIGINT sortkey,
                         hour SMALLINT,
                         day SMALLINT,
                         week SMALLINT,
                         month SMALLINT,
                         year SMALLINT,
                         weekday SMALLINT,
                         PRIMARY KEY(start_time))
                         diststyle all;"""
                    )
# STAGING TABLES
staging_events_copy = ("""copy staging_events_table from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}'
region 'us-west-2'
JSON 'auto' truncatecolumns
;""").format('arn:aws:iam::787896866770:role/dar1en-role-iam')

staging_songs_copy = ("""copy staging_songs_table from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}' 
region 'us-west-2' 
JSON 'auto' truncatecolumns;""").format('arn:aws:iam::787896866770:role/dar1en-role-iam')

# INSERT TABLES
songplay_table_insert = ("""INSERT INTO songplay_table (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent) 
                            SELECT  staging_events_table.ts, 
                                    staging_events_table.userId, 
                                    staging_events_table.level, 
                                    joined_song_artist_table.song_id, 
                                    joined_song_artist_table.artist_id, 
                                    staging_events_table.sessionId,
                                    staging_events_table.location, 
                                    staging_events_table.userAgent
                            FROM staging_events_table 
                            JOIN (
                              SELECT  song_table.song_id,
                                      artist_table.artist_id,
                                      song_table.title,
                                      artist_table.name,
                                      song_table.duration
                              FROM song_table 
                              JOIN artist_table 
                              ON song_table.artist_id = artist_table.artist_id) joined_song_artist_table
                            ON staging_events_table.song = joined_song_artist_table.title
                            AND staging_events_table.artist = joined_song_artist_table.name
                            AND staging_events_table.length = joined_song_artist_table.duration;""")
user_table_insert = ("""INSERT INTO user_table (
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
                        SELECT userId, firstName, lastName, gender, level
                        FROM staging_events_table
                        WHERE page = 'NextSong'
                        ;""")

song_table_insert = ("""INSERT INTO song_table (
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration)
                        SELECT DISTINCT(song_id), title, artist_id, year, duration
                        FROM staging_songs_table
                        ;""")

artist_table_insert = ("""INSERT INTO artist_table (
                          artist_id,
                          name,
                          location,
                          lattitude,
                          longitude) 
                          SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs_table
                          ;""")

time_table_insert = ("""INSERT INTO time_table (
                        start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
                        SELECT  DISTINCT(ts), 
                                EXTRACT(hour from timestamp 'epoch' + (ts/1000) * interval '1 second') as hour, 
                                EXTRACT(day from timestamp 'epoch' + (ts/1000) * interval '1 second') as day, 
                                EXTRACT(week from timestamp 'epoch' + (ts/1000) * interval '1 second') as week,
                                EXTRACT(month from timestamp 'epoch' + (ts/1000) * interval '1 second') as month, 
                                EXTRACT(year from timestamp 'epoch' + (ts/1000) * interval '1 second') as year,
                                EXTRACT(weekday from timestamp 'epoch' + (ts/1000) * interval '1 second') as weekday
                        FROM staging_events_table
                        ;""")


# QUERY LISTS

create_schema_queries         = [nodist_schema_create, dist_schema_create]
set_schema_queries            = [nodist_schema_set, dist_schema_set]
create_table_queries          = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
create_no_dist_table_queries  = [no_dist_staging_events_table_create, no_dist_staging_songs_table_create, no_dist_user_table_create, no_dist_song_table_create, no_dist_artist_table_create, no_dist_time_table_create, no_dist_songplay_table_create]
drop_table_queries            = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries            = [staging_events_copy, staging_songs_copy]
insert_table_queries          = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]


