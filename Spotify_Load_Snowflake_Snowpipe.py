CREATE DATABASE spotify_db;

// Secure connection between snowflake and S3
CREATE OR REPLACE storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::536697254703:role/spotify-spark-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://etl-project-spotify')
    COMMENT = 'Creating connection to S3'

// To Retrieve STORAGE_AWS_IAM_USER_ARN and EXTERNAL_ID to add to the IAM Role to create the connection
// between S3 and Snowflake
DESC integration s3_init;


// Creating the file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ","
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

CREATE OR REPLACE stage spotify_stage //stage is used to create the connection using the storage integration to access the s3 data
    url = 's3://etl-project-spotify/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat;

LIST @spotify_stage/songs;

CREATE OR REPLACE TABLE tbl_album(
    album_id STRING,
    name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING

);

CREATE OR REPLACE TABLE tbl_artist(
    artist_id STRING,
    name STRING,
    url STRING

);

CREATE OR REPLACE TABLE tbl_songs(
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING

);

SELECT * FROM tbl_artist;

COPY INTO tbl_album
FROM @spotify_stage/album_data/album_transformed_2024-09-26/run-1727372692508-part-r-00000;

COPY INTO tbl_artist
FROM @spotify_stage/artist_data/artist_transformed_2024-09-26/run-1727372939596-part-r-00012;

COPY INTO tbl_songs
FROM @spotify_stage/songs_data/songs_transformed_2024-09-26/run-1727372940455-part-r-00014;

// Creating snowpipe
CREATE OR REPLACE SCHEMA pipe;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_songs_pipe
auto_ingest = TRUE //automatically runs whenever new file is uploaded
AS 
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs_data/;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE //automatically runs whenever new file is uploaded
AS 
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album_data/;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_artist_pipe
auto_ingest = TRUE //automatically runs whenever new file is uploaded
AS 
COPY INTO spotify_db.public.tbl_artist
FROM @spotify_db.public.spotify_stage/artist_data/;

//Interested in the notification_channel (SQS: Simple Queue Service)
// Any event that is uploaded on S3 bucket, will automatically send a notification from S3 to this channel and will redirect to snowflake
DESC pipe pipe.tbl_songs_pipe;

DESC pipe pipe.tbl_artist_pipe;

DESC pipe pipe.tbl_album_pipe;

SELECT Count(*) FROM tbl_artist;

SELECT Count(*) FROM tbl_album;

SELECT SYSTEM$PIPE_STATUS('pipe.tbl_artist_pipe')