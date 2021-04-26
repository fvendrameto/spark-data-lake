def process_song_data(spark, input_data, output_data):
    songs_df = get_songs_df(spark, input_data)

    # Extract songs table columns
    songs_table = songs_df.dropDuplicates(['song_id']) \
        .select(['song_id', 'title', 'artist_id', 'year', 'duration'])

    # Write songs, partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(output_data + 'songs', mode='overwrite')

    # Extract artists table columns, renaming where necessary
    artists_table_fields = [
        'artist_id', 'artist_name as name', 'artist_location as location',
        'artist_latitude as latitude', 'artist_longitude as longitude'
    ]
    artists_table = songs_df.dropDuplicates(['artist_id']) \
        .selectExpr(artists_table_fields)

    # Write artists table
    artists_table.write.parquet(output_data + 'artists', mode='overwrite')


def get_songs_df(spark, input_data):
    song_data_path = input_data + 'song_data/*/*/*/*.json'
    songs_df = spark.read.json(song_data_path)
    return songs_df
