# Spark Data Lake Project

## Summary

This repository implements an ETL procedure to process data in JSON format to columnar Parquet files. The input files is available on AWS S3 and the output is save on a bucket there as well.

The data was modeled in a Star Schema, with four dimension tables and a fact table:

- `artists`: Dimension table with data from various artists, obtained through the songs dataset.
- `songs`: Dimension table with data from various songs, also obtained from the songs dataset.
- `users`: Dimension table with app user data, obtained from the logs dataset.
- `time`: Dimension table with timestamps references, also obtained from the logs dataset.
- `songplays`: Fact table, with references to all dimensions and additional data obtained from the logs dataset, such as the user session id, the location of this session and the application user agent.

## Project organization

This project is implemented in Python and has several modules, which are described ahead:

- `create_spark_session.py`: loads a file `dl.cfg` with AWS keys to access the source data and save the results on S3.
- `etl.py`: using Spark's Python API (pyspark), performs ETL on the dataset to generate the tables mentioned earlier.
- `process_logs.py`: implements the procedures to generate the users and time tables from logs data and songplays table from both datasets.
- `process_songs.py`: implements the procedures to generate artists and songs tables.

## Execution

This project is meant to run on Udacity's environment, with access to S3 buckets which may be private or on a AWS EMR cluster.

There is a single executable script in this project and running it is straightforward:

```bash
python etl.py
```

The results as set to be written in overwrite mode, so each execution will clear all data previously generated.
# spark-data-lake
