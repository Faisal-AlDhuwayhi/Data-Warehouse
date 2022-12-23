import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth text,
        first_name text,
        gender varchar,
        item_session integer,
        last_name text,
        length decimal,
        level varchar,
        location text,
        method varchar,
        page varchar,
        registration decimal,
        session_id integer,
        song text,
        status smallint,
        ts bigint,
        user_agent text,
        user_id integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id text,
        title text,
        duration decimal,
        year smallint,
        num_songs integer,
        artist_id text,
        artist_latitude real,
        artist_longitude real,
        artist_location text,
        artist_name text
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY sortkey,
        first_name text,
        last_name text,
        gender varchar,
        level varchar
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text PRIMARY KEY distkey,
        title varchar NOT NULL,
        artist_id text,
        year smallint sortkey,
        duration numeric NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id text PRIMARY KEY,
        name text NOT NULL sortkey,
        location varchar,
        latitude real,
        longitude real
    ) 
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL PRIMARY KEY sortkey,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY sortkey,
        start_time timestamp NOT NULL REFERENCES time(start_time),
        user_id int NOT NULL REFERENCES users(user_id),
        level varchar,
        song_id text NOT NULL REFERENCES songs(song_id) distkey,
        artist_id text NOT NULL REFERENCES artists(artist_id),
        session_id int,
        location text,
        user_agent text
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    JSON {}
    REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT AS JSON 'auto'
    REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e
    JOIN staging_songs s
    ON e.song = s.title AND e.artist = s.artist_name AND ABS(e.length - s.duration) < 2
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT(user_id) AS user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE user_id IS NOT NULL AND page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT
        DISTINCT(artist_id) AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') AS ts FROM staging_events)
    SELECT
        DISTINCT(ts) AS start_time,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(week FROM ts) AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(weekday FROM ts) AS weekday
    FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
