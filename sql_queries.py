import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS stage_logs"
staging_songs_table_drop = "DROP table IF EXISTS stage_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS stage_logs (
                                    artist text,
                                    auth text,
                                    firstName text,
                                    gender text,
                                    iteminSession int,
                                    lastName text,
                                    length numeric,
                                    level text,
                                    location text,
                                    method text,
                                    page text ,
                                    registration bigint,
                                    sessionID int,
                                    song text,
                                    status int,
                                    ts bigint,
                                    userAgent text,
                                    userID int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stage_songs (
                                    num_songs int,
                                    artist_id text,
                                    artist_latitude text,
                                    artist_longitude text,
                                    artist_location text,
                                    artist_name text,
                                    song_id text,
                                    title text,
                                    duration text,
                                    year int)
""")


songplay_table_create =  ("""CREATE TABLE IF NOT EXISTS songplays (
                                    songplay_id int IDENTITY(0,1) PRIMARY KEY,
                                    start_time text NOT NULL,
                                    user_id text NOT NULL,
                                    level text,
                                    song_id text,
                                    artist_id text,
                                    session_id text,
                                    location text,
                                    user_agent text)

""")

user_table_create =  ("""CREATE TABLE IF NOT EXISTS users (
                                    user_id text PRIMARY KEY,
                                    first_name text,
                                    last_name text,
                                    gender text,
                                    level text)

""")

song_table_create =  ("""CREATE TABLE IF NOT EXISTS songs (
                                    song_id text PRIMARY KEY,
                                    title text,
                                    artist_id text,
                                    year int,
                                    duration text)

""")

artist_table_create =  ("""CREATE TABLE IF NOT EXISTS artists (
                                    artist_id text PRIMARY KEY,
                                    name text,
                                    location text,
                                    latitude text,
                                    longitude text)

""")
 
time_table_create =  ("""CREATE TABLE IF NOT EXISTS time (
                                    start_time text PRIMARY KEY,
                                    hour int,
                                    day int,
                                    week int,
                                    month int,
                                    year int,
                                    weekday int)
""")

# STAGING TABLES

staging_events_copy = ('''copy stage_logs from {}
                            credentials 'aws_iam_role={}'
                            compupdate off region 'us-west-2'
                            format as json {};
''').format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ('''copy stage_songs from {}
                            credentials 'aws_iam_role={}'
                            compupdate off region 'us-west-2'
                            format as json 'auto';
''').format(config[SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT TIMESTAMP 'epoch' + l.ts/1000 * INTERVAL '1 second' AS start_time, l.userID, s.song_id, s.artist_id, l.sessionId, l.location, l.userAgent 
                            FROM stage_logs l, stage_songs s
                            WHERE l.artist = s.artist_name AND l.song = s.title AND l.page = 'NextSong'                            
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                        SELECT DISTINCT userID, firstName, lastName, gender, level
                        FROM stage_logs 
                        WHERE ts IN 
                        (SELECT MAX(ts) from stage_logs 
                        WHERE page = 'NextSong' 
                        GROUP BY userid)'
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year)
                        SELECT DISTINCT song_id, title, artist_id, year
                        FROM stage_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                        FROM stage_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, 
                            extract(hour from start_time) AS hour,
                            extract(day from start_time) AS day,
                            extract(week from start_time) AS week,
                            extract(month from start_time) AS month,
                            extract(year from start_time) AS year,
                            extract(weekday from start_time) AS weekday
                        FROM stage_logs
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
