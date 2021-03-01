#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f


if __name__ == "__main__":

    spark = SparkSession.builder.appName("aggregate-tweets-sentiments").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # load data from staging and sentiments
    staging_data = spark.read.json('s3://twitter-data-sm/staging/*.json').drop('withheld')
    sentiment_data = spark.read.json('s3://twitter-data-sm/sentiments/*.json').dropDuplicates(['tweet_id'])

    # merge tweet metadata and sentiments
    data = staging_data.join(sentiment_data, "tweet_id")

    # write data
    data.write.format('csv').save('s3://twitter-data-sm/redshift/all_tweets.csv', header=True)

    # change datatype from string to timestamp for created_at column
    data = data.withColumn('created_at',f.to_timestamp("created_at", "yyyy/MM/dd-HH:mm:ss"))

    # create 15 mins windows
    seconds = 15 * 60
    seconds_window = f.from_unixtime(f.unix_timestamp('created_at') - f.unix_timestamp('created_at') % seconds)
    data = data.withColumn('15_min_window', seconds_window)

    # take avg score for window
    scores_avg = data.groupby('15_min_window').agg(
        f.round(f.avg('positive'), 4).alias('avg_positive'),
        f.round(f.avg('negative'), 4).alias('avg_negative'),
        f.round(f.avg('neutral'), 4).alias('avg_neutral'),
        f.round(f.avg('mixed'), 4).alias('avg_mixed'),
    ).withColumnRenamed('15_min_window', 'window')

    # count labels per window
    label_counts = data.groupby(['15_min_window']).count()

    # pivoted label count per window and label showing percetage count per window per label
    label_counts_pivot = data.groupby(['15_min_window']).pivot('label').agg(f.count('label'))
    label_counts = label_counts_pivot.join(label_counts, "15_min_window")\
        .withColumn("perc_tweet_mixed", f.round(f.col("MIXED")/f.col("count"), 4))\
        .withColumn("perc_tweet_positive", f.round(f.col("POSITIVE")/f.col("count"), 4))\
        .withColumn("perc_tweet_negative", f.round(f.col("NEGATIVE")/f.col("count"), 4))\
        .withColumn("perc_tweet_neutral", f.round(f.col("NEUTRAL")/f.col("count"), 4))\
        .withColumnRenamed('15_min_window', 'window')\
        .drop('MIXED', 'NEGATIVE', 'POSITIVE', 'NEUTRAL')

    # join both aggregated dataframes
    agg_data = scores_avg.join(label_counts, "window").withColumnRenamed('count', 'count_tweets').withColumnRenamed('window', 'window_time')

    # write aggregated data to s3
    agg_data.write.format('csv').save('s3://twitter-data-sm/redshift/agg_data.csv', header=True)
        


    spark.stop()