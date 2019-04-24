import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

# First two tables are staging tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"

# Dimension & Fact tables
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table (
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
                                  registration FLOAT,
                                  sessionId INT,
                                  song TEXT,
                                  status SMALLINT,
                                  ts BIGINT,
                                  userAgent TEXT,
                                  userId INT);"""
                             )

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table
                                 (num_songs SMALLINT,
                                  artist_id TEXT,
                                  artist_latitude FLOAT,
                                  artist_longitude FLOAT,
                                  artist_location TEXT,
                                  artist_name TEXT,
                                  song_id TEXT,
                                  title TEXT,
                                  duration FLOAT,
                                  year SMALLINT);"""
                             )

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table
                            (songplay_id INT IDENTITY(0,1),
                             start_time BIGINT sortkey,
                             user_id INT,
                             level VARCHAR(10),
                             song_id TEXT distkey,
                             artist_id TEXT,
                             session_id INT,
                             location TEXT,
                             user_agent TEXT);"""
                        )

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
                        (user_id INT,
                         first_name TEXT, 
                         last_name TEXT,
                         gender VARCHAR(5),
                         level VARCHAR(10))
                         diststyle all;"""
                    )

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table
                        (song_id TEXT distkey,
                         title TEXT,
                         artist_id TEXT,
                         year SMALLINT,
                         duration FLOAT);"""
                    )

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table
                          (artist_id TEXT,
                           name TEXT,
                           location TEXT,
                           lattitude FLOAT,
                           longitude FLOAT)
                           diststyle all;"""
                           )

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table
                        (start_time BIGINT sortkey,
                         hour SMALLINT,
                         day SMALLINT,
                         week SMALLINT,
                         month SMALLINT,
                         year SMALLINT,
                         weekday SMALLINT)
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

# FINAL TABLES - Inserting records in dimension tables

songplay_table_insert = ("""INSERT INTO songplay_table (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent) 
                            SELECT staging_events_table.ts, staging_events_table.userId, staging_events_table.level, 
                            song_table.song_id, artist_table.artist_id, 
                            staging_events_table.sessionId,staging_events_table.location,staging_events_table.userAgent
                            FROM song_table JOIN artist_table ON song_table.artist_id = artist_table.artist_id
                            JOIN staging_events_table ON staging_events_table.song = song_table.title 
                            AND staging_events_table.artist = artist_table.name
                            AND staging_events_table.length = song_table.duration;""")

user_table_insert = ("""INSERT INTO user_table (
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
                        SELECT userId, firstName, lastName, gender, level
                        FROM staging_events_table
                        WHERE page = 'NextSong';""")

song_table_insert = ("""INSERT INTO song_table (
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration)
                        SELECT DISTINCT(song_id), title, artist_id, year, duration
                        FROM staging_songs_table;""")

artist_table_insert = ("""INSERT INTO artist_table (
                          artist_id,
                          name,
                          location,
                          lattitude,
                          longitude) 
                          SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs_table;""")

time_table_insert = ("""INSERT INTO time_table (
                        start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
                        SELECT DISTINCT(ts),
                        EXTRACT(hour from timestamp 'epoch' + (ts/1000) * interval '1 second') as hour,
                        EXTRACT(day from timestamp 'epoch' + (ts/1000) * interval '1 second') as day,
                        EXTRACT(week from timestamp 'epoch' + (ts/1000) * interval '1 second') as week,
                        EXTRACT(month from timestamp 'epoch' + (ts/1000) * interval '1 second') as month,
                        EXTRACT(year from timestamp 'epoch' + (ts/1000) * interval '1 second') as year, 
                        EXTRACT(dow from timestamp 'epoch' + (ts/1000) * interval '1 second') as weekday
                        FROM staging_events_table;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
