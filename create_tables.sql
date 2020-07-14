DROP TABLE IF EXISTS public.authors;
DROP TABLE IF EXISTS public.comments;
DROP TABLE IF EXISTS public.posts;
DROP TABLE IF EXISTS public.reviews;

CREATE TABLE public.authors (
    author_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(256),
    meibi VARCHAR(256),
    meibx VARCHAR(256),
    avg_num_words_post DOUBLE PRECISION,
    avg_num_words_post_nostopword DOUBLE PRECISION
)

CREATE TABLE public.comments (
    comment_id VARCHAR(256) PRIMARY KEY,
    post_id VARCHAR(256),
    content VARCHAR(256),
    author VARCHAR(256),
    date DATETIME,
    vote INTEGER
)

CREATE TABLE public.posts (
    post_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(256),
    blogger_name VARCHAR(256),
    blogger_id VARCHAR(256),
    number_of_comments INTEGER,
    content VARCHAR(256),
    url VARCHAR(256),
    date DATETIME,
    number_of_retrieved_comments INTEGER
)

CREATE TABLE public.authors (
    author_id VARCHAR(256) PRIMARY KEY,
    blogger_id VARCHAR(256),
    post_id VARCHAR(256),
    comment_id VARCHAR(256),
    date DATETIME,
    comment VARCHAR(256),
    sentiment VARCHAR(256)
)