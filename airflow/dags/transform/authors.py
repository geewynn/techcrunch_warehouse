from pyspark.sql import SparkSession


spark = SparkSession \
        .builder \
        .appName('Wrangling Data') \
        .getOrCreate()


author_df = spark.read.csv('s3a://bloggers-data/author/authors.csv')
author_df.createOrReplaceTempView('authors')

# transfrom data for author table
author_table = spark.sql("""
select authors._c0 author_id,
        authors._c1 author,
        authors._c2 meibi,
        authors._c3 meibix
from authors
""")
author_table.write.parquet('s3a://bloggers-data/temp/author.parquet')

# transform data for word_count table
word_count_table = spark.sql("""
select authors._c1 author,
       authors._c0 author_id,
       authors._c4 word_count_stopwords,
       authors._c5 word_count_nostopwords
from authors
""")
word_count_table.write.parquet('s3a://bloggers-data/temp/word_count.parquet')