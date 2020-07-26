DROP TABLE IF EXISTS public.author;
DROP TABLE IF EXISTS public.comments;
DROP TABLE IF EXISTS public.posts;
DROP TABLE IF EXISTS public.comment_review;
DROP TABLE IF EXISTS public.word_count;
DROP TABLE IF EXISTS public.script_log;

CREATE TABLE public.author (
    author_id VARCHAR(256) PRIMARY KEY,
    author VARCHAR(256),
    meibi VARCHAR(256),
    meibx VARCHAR(256)
);

CREATE TABLE public.comments (
    comment_id VARCHAR(256) PRIMARY KEY,
    post_id VARCHAR(65000),
    content VARCHAR(65535),
    author VARCHAR(65000),
    date VARCHAR(65000),
    vote VARCHAR(65000)
);

CREATE TABLE public.posts (
    post_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(65000),
    blogger_name VARCHAR(65000),
    blogger_id VARCHAR(65000),
    number_of_comments VARCHAR(256),
    content VARCHAR(65535),
    url VARCHAR(65000),
    date VARCHAR(65000),
    number_of_retrieved_comments VARCHAR(5000)
);

CREATE TABLE public.word_count (
    author_id VARCHAR(256) PRIMARY KEY,
    author VARCHAR(1000),
    word_count_stopwords VARCHAR,
    word_count_nostopwords VARCHAR
);

CREATE TABLE public.comment_review (
    author_id VARCHAR(256) PRIMARY KEY,
    author VARCHAR(1000),
    post_id VARCHAR(256),
    comment_id VARCHAR(256),
    date VARCHAR,
    content VARCHAR(15000),
    sentiment VARCHAR(256)
);

CREATE TABLE public.script_log (
  script_name varchar(32) NOT NULL,
  processed_file smallint
);