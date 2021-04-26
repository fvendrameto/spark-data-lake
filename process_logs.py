from datetime import datetime

from pyspark.sql.functions import (monotonically_increasing_id, to_timestamp,
                                   udf)
from process_songs import get_songs_df


def timestamp_to_datetime(timestamp):
    datetime_format = '%Y-%m-%d %H:%M:%S'
    return datetime.fromtimestamp(timestamp / 1000).strftime(datetime_format)


def process_log_data(spark, input_data, output_data):
    log_data_path = input_data + 'log_data/*/*/*.json'

    # Read log data and filter by songplay entries
    log_df = spark.read.json(log_data_path).where("page = 'NextSong'")

    # Extract users table columns, selecting distinct entries and renaming
    # where necessary
    users_table_fields = [
        'userId as user_id', 'firstName as first_name',
        'lastName as last_name', 'gender', 'level'
    ]
    users_table = log_df.dropDuplicates(['userId']) \
        .selectExpr(users_table_fields)

    # Write users table
    users_table.write.parquet(output_data + 'users', mode='overwrite')

    # Create date_time and start_time column from original timestamp column
    get_datetime = udf(timestamp_to_datetime)
    log_df = log_df.withColumn('date_time', get_datetime('ts'))
    log_df = log_df.withColumn('start_time', to_timestamp('date_time'))

    # Extract time table columns, selecting distinct entries and renaming
    # where necessary
    time_table_fields = [
        'start_time', 'hour(start_time) as hour', 'day(start_time) as day',
        'weekofyear(start_time) as week', 'month(start_time) as month',
        'year(start_time) as year', 'dayofweek(start_time) as weekday'
    ]
    time_table = log_df.dropDuplicates(['ts']).selectExpr(time_table_fields)

    # Write time table
    time_table.write.partitionBy(['year', 'month']) \
        .parquet(output_data + 'time', mode='overwrite')

    songs_df = get_songs_df(spark, input_data)

    # Join logs and songs data frames
    join_dfs_expression = (log_df.song == songs_df.title) \
                        & (log_df.artist == songs_df.artist_name)
    log_and_songs_df = log_df.join(songs_df, join_dfs_expression)

    # Extract songplays table, creating and incremental id
    songplays_fields = [
        'start_time', 'userId as user_id', 'level', 'song_id', 'artist_id',
        'sessionId as session_id', 'location', 'userAgent as user_agent'
    ]
    songplays_table = log_and_songs_df.selectExpr(songplays_fields) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # Write songplays table, partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']) \
        .parquet(output_data + 'songplays', mode='overwrite')
