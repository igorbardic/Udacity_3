import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay cascade"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
  artist         varchar(255)      ,    
  auth           varchar(50)       ,
  first_name     varchar(255)      ,
  gender         varchar(1)        ,
  iteminsession  integer           ,
  last_name      varchar(255)      ,
  lenght         DOUBLE PRECISION  ,
  level          varchar(50)       ,
  location       varchar(255)      ,
  method         varchar(25)       ,
  page           varchar(35)       ,
  registration   VARCHAR(50)       ,
  sessionid      bigint            ,
  song           varchar(255)      ,
  status         integer           ,
  ts             bigint            ,
  user_agent     varchar(200)      ,
  user_id        integer           );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
  songs_id          varchar(255)    ,
  num_songs         integer         ,    
  artist_id         varchar(255)    ,
  artist_latitude   numeric         ,
  artist_longitude  numeric         ,
  artist_location   varchar(200)    ,
  artist_name       varchar(30)     ,
  song_id           varchar(255)    ,
  title             varchar(255)    ,
  duration          numeric         ,
  year              integer         );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
  sp_songplay_id    BIGINT IDENTITY(0,1),
  sp_start_time     TIMESTAMP       not null,
  sp_user_id        integer         not null,
  sp_level          varchar(50)     ,
  sp_song_id        varchar(255)    not null,
  sp_artist_id      varchar(255)    not null,
  sp_session_id     bigint          ,
  sp_location       varchar(255)    ,
  sp_user_agent     varchar(200)    ,
  PRIMARY KEY (sp_songplay_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
  u_user_id        integer         not null,
  u_first_name     varchar(255)    ,
  u_last_name      varchar(255)    ,
  u_gender         varchar(1)      ,
  u_level          varchar(50)     ,
  PRIMARY KEY (u_user_id)) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
  s_song_id        varchar(255)    not null,
  s_title          varchar(255)    ,
  s_artist_id      varchar(255)    not null,
  s_year           integer         ,
  s_duration       integer         ,
  PRIMARY KEY (s_song_id)) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
  a_artist_id        varchar(255)  not null,
  a_name             varchar(255)  ,
  a_location         varchar(255)  ,
  a_latitude         numeric       ,
  a_longitude        numeric       ,
  PRIMARY KEY (a_artist_id)) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  t_start_time        TIMESTAMP   not null,
  t_hour              integer     ,
  t_day               integer     ,
  t_week              integer     ,
  t_month             integer     ,
  t_year              integer     ,
  t_weekday           integer     ,
  PRIMARY KEY (t_start_time)) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                            from {} credentials  
                            'aws_iam_role={}'   
                            json {}  
                            compupdate off
                            region 'us-west-2';
""").format(config.get("S3","LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs 
                            from {} credentials
                            \'aws_iam_role={}\'
                            JSON 'auto'
                            truncatecolumns compupdate off 
                            region \'us-west-2\';
""").format(config.get("S3","SONG_DATA"),
            config.get("IAM_ROLE", "ARN"))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, sp_location, sp_user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.user_agent 
    FROM staging_events se
    INNER JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong' AND start_time IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (u_user_id, u_first_name, u_last_name, u_gender, u_level)
SELECT DISTINCT user_id,
                first_name,
                last_name,
                gender,
                level 
                FROM staging_events se
                WHERE se.page = 'NextSong' AND se.user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (s_song_id, s_title, s_artist_id, s_year, s_duration)
SELECT DISTINCT song_id,
       title,
       artist_id,
       year,
       duration
       FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artist (a_artist_id, a_name, a_location, a_latitude, a_longitude)
SELECT DISTINCT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
       FROM staging_songs;
""")

time_table_insert = ("""
  INSERT INTO time (t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday)
      Select distinct t_start_time,
                      EXTRACT(HOUR FROM t_start_time) As t_hour,
                      EXTRACT(DAY FROM t_start_time) As t_day,
                      EXTRACT(WEEK FROM t_start_time) As t_week,
                      EXTRACT(MONTH FROM t_start_time) As t_month, 
                      EXTRACT(YEAR FROM t_start_time) As t_year,
                      EXTRACT(DOW FROM t_start_time) As t_weekday
                      FROM (SELECT distinct ts, TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as t_start_time
                      FROM staging_events);
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
