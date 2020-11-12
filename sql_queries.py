import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay cascade"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
  artist         varchar(50)       not null    
  auth           varchar(30)       not null,
  first_name     varchar(30)       not null,
  gender         varchar(5)        not null,
  iteminsession  integer           not null,
  last_name      varchar(30)       not null,
  lenght         decimal           ,
  level          varchar(20)       not null,
  locatioan      varchar(200)      not null,
  method         varchar(10)       not null,
  page           varchar(20)       not null,
  registration   integer           not null,
  sessionid      integer           not null,
  song           varchar(200)      not null,
  status         integer           not null,
  ts             integer           not null,
  user_agent     varchar(200)      not null,
  user_id        integer           not null
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
  num_songs         integer         not null    
  artist_id         integer         not null,
  artist_latitude   decimal         ,
  artist_longitude  decimal         ,
  artist_location   varchar(200)    ,
  artist_name       varchar(30)     not null,
  song_id           integer         not null,
  title             varchar(200)    not null,
  duration          decimal         not null,
  year              integer         not null
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
  sp_songplay_id    integer         not null    sortkey distkey,
  sp_start_time     varchar(30)     not null,
  sp_user_id        integer         not null,
  sp_level          varchar(20)     not null,
  sp_song_id        integer         not null,
  sp_artist_id      integer         not null,
  sp_session_id     integer         not null,
  sp_location       varchar(200)    not null,
  sp_user_agent     varchar(200)    not null,
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user (
  u_user_id        integer         not null    sortkey,
  u_first_name     varchar(30)     not null,
  u_last_name      varchar(30)     not null,
  u_gender         varchar(5)      not null,
  u_level          varchar(20)     not null
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
  s_song_id        integer         not null    sortkey,
  s_title          varchar(200)    not null,
  s_artist_id      integer         not null,
  s_year           integer         not null,
  s_duration       integer         not null
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
  a_artist_id        integer         not null    sortkey,
  a_name             varchar(50)     not null,
  a_location         varchar(50)     ,
  a_latitude         varchar(50)     ,
  a_longitude        varchar(50)     
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  t_ts                integer     not null    sortkey,
  t_start_time        integer     not null,
  t_hour              integer     not null,
  t_day               integer     not null,
  t_week              integer     not null,
  t_month             integer     not null
  t_year              integer     not null
  t_weekday           integer     not null
) diststyle all;
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
INSERT INTO songplay(sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, sp_location, sp_user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent 
    FROM staging_events se
    INNER JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong' AND start_time IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO user(u_user_id, u_first_name, u_last_name, u_gender, u_level)
SELECT DISTINCT userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level 
                FROM staging_events se
                WHERE se.page = 'NextSong' AND se.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (s_song_id, s_title, s_artist_id, s_year, s_duration)
SELECT song_id,
       title,
       artist_id,
       year,
       duration
       FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artist (a_artist_id, a_name, a_location, a_latitude, a_longitude)
SELECT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude,
       FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time(t_ts,t_start_time,t_hour,t_day,t_week,t_month,t_year,t_weekday)
Select  distinct ts,
        t_start_time,
        EXTRACT(HOUR FROM t_start_time) As t_hour,
        EXTRACT(DAY FROM t_start_time) As t_day,
        EXTRACT(WEEK FROM t_start_time) As t_week,
        EXTRACT(MONTH FROM t_start_time) As t_month,
        EXTRACT(YEAR FROM t_start_time) As t_year,
        EXTRACT(DOW FROM t_start_time) As t_weekday,
        FROM (
        SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as t_start_time
        FROM staging_events
        )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
