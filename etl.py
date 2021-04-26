from create_spark_session import get_spark_session
from process_logs import process_log_data
from process_songs import process_song_data


def process_data():
    spark = get_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://output-data/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    process_data()
