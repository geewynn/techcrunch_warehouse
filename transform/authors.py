from pyspark.sql import SparkSession


author_df = spark.read.csv('authors.csv')
author_df.createOrReplaceTempView('authors')

# transfrom data for author table
author_table = spark.sql("""
select authors._c0 author_id,
        authors._c1 author,
        authors._c2 meibi,
        authors._c3 meibix
from authors
""")
author_table.write.parquet('author.parquet')

# transform data for word_count table
word_count_table = spark.sql("""
select authors._c1 author,
       authors._c0 author_id,
       authors._c4 word_count_stopwords,
       authors._c5 word_count_nostopwords
from authors
""")
word_count_table.write.parquet('word_count.parquet')