# Base analysis

We have two big data file - song_data with songs and artist data, and log_data with streaming data of users events.
Our goal is create one fact table songsplays with time data and four dimensions table with other data (users, songs, artists, time).

Before that we should do create two staging with big data from S3 to Amazon Redshift DWH, and with ETL procedure load our table.
Finnaly we test our data with sql query on Amazon Redshift.


# 1.Create Cluster on AWS and configure dwh.cfg with parameters

# 2.Write sql Drop, Create, Copy Staging and Insert queries to skript sql_queries.py

# 3.Run create_table.py over console - !python create_tables.py or run over terminal python create_tables.py

# 4.Run etl.py over console - !python etl.py or run over terminal python etl.py

# 5.Test insert data with sql query editor in AWS Redshift

select * from songplay where sp_user_id = 10;

select count(*) from songplay; - we have 329 songplays users who played songs 

select count(*) from songs; - we have 44688 songs in database

select count(*) from users; - we have 104 users in database


