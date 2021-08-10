import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    
    Load data from song_data dataset and extract columns
    for songs and artist tables and write the data into parquet
    files which will be loaded on s3.
    Parameters
    ----------
    spark:  This is the spark session that has been created.
    input_data: Location of songs json files on S3 bucket.     
    output_data: This is the path where the dimentional tables in parquet files will be stored       on S3.
    
    """
    # get filepath to song data file
    song_data =os.path.join(input_data , "song-data/*/*/*.json") 
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id','title','artist_id','year','duration']
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data,             'songs.parquet'),'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude',             'artist_longitude']
    artists_table= artists_table.dropDuplicates(['artist_id'])                
                       
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artist.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 bucket and processes it by extracting songs and artist tables and then loades back to S3 Bucket. Also output from previous function is used in by spark.read.json command
        
        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data.
            output_data : S3 bucket are dimensional tables in parquet format will be stored.
            
    """
    # get filepath to log data file
    log_data =os.path.join(input_data, "log_data/*/*/*.json")
    

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    song_plays = df.filter(df.page == 'NextSong') \
                    .select('ts', 'userId', 'level','sessionId', 'location', 'userAgent')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table=users_table.dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("users.parquet Load Completed")
                       
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
                       .withColumn('start_time', df.datetime) \
                       .withColumn('hour', hour('datetime')) \
                       .withColumn('day', dayofmonth('datetime')) \
                       .withColumn('week', weekofyear('datetime')) \
                       .withColumn('month', month('datetime')) \
                       .withColumn('year', year('datetime')) \
                       .withColumn('weekday', dayofweek('datetime'))\
                       .dropDuplicates()
                                   
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+'time/time.parquet')                              
    print("time_table parquet Completed")
          
    # read in song data to use for songplays table
          
    song_df =spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song)
    songplays_table = df.select(
                        col('ts').alias('ts'),
                        col('userId').alias('user_id'),
                        col('level').alias('level'),
                        col('song_id').alias('song_id'),
                        col('artist_id').alias('artist_id'),
                        col('ssessionId').alias('session_id'), 
                        col('location').alias('location'),
                        col('userAgent').alias('user_agent'), 
                        col('year').alias('year'),
                        month('datetime').alias('month')
            )
    
    songplays_table = songplays_table.selectExpr("ts as start_time")
    songplays_table.select(monotinically_increasing_id().alias('songplay_id')).collect()
          
          
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,             'songplays.parquet'), 'overwrite' )
    print("songs_play parquet completed")
    print("Process log data completed")
        


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacityoutput"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
