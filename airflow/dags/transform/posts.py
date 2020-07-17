from pyspark.sql import SparkSession

post_df = spark.read.csv('posts.csv')
post_df.createOrReplaceTempView('posts')

# transform data for post table
post_table = spark.sql("""
select posts._c0 post_id,
       posts._c1 title,
       posts._c2 blogger_name,
       posts._c3 blogger_id,
       posts._c4 number_of_comments,
       posts._c5 content,
       posts._c6 url,
       posts._c7 date,
       posts._c8 number_of_retrieved_comments
from posts
""")
post_table.write.parquet('posts.parquet')