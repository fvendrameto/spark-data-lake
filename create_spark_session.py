import configparser
import os

from pyspark.sql import SparkSession


def get_spark_session():
    setup_aws_keys()

    spark = SparkSession.builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def setup_aws_keys():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET_ACCESS_KEY']
