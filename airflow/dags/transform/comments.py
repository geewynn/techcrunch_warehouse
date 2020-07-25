from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import re

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')


# load data and create a temporary table
comment_df = spark.read.csv('s3a://bloggers-data/comments/comments.csv')
author_df = spark.read.csv('s3a://bloggers-data/author/authors.csv')
author_df.createOrReplaceTempView('authors')
comment_df.createOrReplaceTempView('comments')

# transform data for comment table
comment_table = spark.sql("""
select comments._c0 comment_id,
       comments._c1 post_id,
       comments._c2 content,
       comments._c3 aurthor,
       comments._c4 date,
       comments._c5 vote
from comments
""")
# write comment table to parquet
comment_table.write.parquet('s3a://tech-temp/comments.parquet')


# set staging table for comment review
review_staging_df = spark.sql("""
select authors._c1 author,
        comments._c3 author_id,
        comments._c1 post_id,
        comments._c0 comment_id,
        comments._c4 date,
        lower(comments._c2) content
from comments
join authors on authors._c0 = comments._c3
where comments._c3 is not null and comments._c2 is not null
""")


# Build the sentiment analyzer
analyser = SentimentIntensityAnalyzer()
def get_sentiment_analysis_score(sentence):
    score = analyser.polarity_scores(sentence)
    return score['compound']

def get_sentiment_analysis_result(score):
    if score >= 0.05:
        return "POSITIVE"
    elif score <= -0.05:
        return "NEGATIVE"
    else:
        return "NEUTRAL"
    
get_sentiment_analysis_score_udf = F.udf(lambda x: get_sentiment_analysis_score(x), DoubleType())
get_sentiment_analysis_result_udf = F.udf(lambda x: get_sentiment_analysis_result(x), StringType())


# Load review data and write a parquet
comment_review_table_df = review_staging_df \
    .withColumn("sa_score", get_sentiment_analysis_score_udf("content")) \
    .withColumn("sentiment", get_sentiment_analysis_result_udf("sa_score")) \
    .select(
        "author_id",
        "author",
        "post_id",
        "comment_id",
        "date",
        "content",
        "sentiment")

# write to parquet
comment_review_table_df.write.parquet('s3a://tech-temp/comments.parquet')