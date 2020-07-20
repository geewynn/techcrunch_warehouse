DROP TABLE IF EXISTS public.authors;
DROP TABLE IF EXISTS public.comments;
DROP TABLE IF EXISTS public.posts;
DROP TABLE IF EXISTS public.reviews;
DROP TABLE IF EXISTS public.comment_review;
DROP TABLE IF EXISTS public.word_count;
DROP TABLE IF EXISTS public.script_log;

CREATE TABLE public.authors (
    author_id VARCHAR(256) PRIMARY KEY,
    author VARCHAR(256),
    meibi VARCHAR(256),
    meibx VARCHAR(256)
);

CREATE TABLE public.comments (
    comment_id VARCHAR(256) PRIMARY KEY,
    post_id VARCHAR(256),
    content VARCHAR(256),
    author VARCHAR(256),
    date DATETIME,
    vote INTEGER
);

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
);

CREATE TABLE public.word_count (
    author_id VARCHAR(256) PRIMARY KEY,
    author VARCHAR(256),
    word_count_stopwords DOUBLE PRECISION,
    word_count_nostopwords DOUBLE PRECISION
);

CREATE TABLE public.comment_review (
    author_id VARCHAR(256) PRIMARY KEY,
    author VARCHAR(256),
    post_id VARCHAR(256),
    blogger_id VARCHAR(256),
    comment_id VARCHAR(256),
    date DATETIME,
    content VARCHAR(256),
    sentiment VARCHAR(256)
);

CREATE TABLE public.script_log (
  script_name varchar(32) NOT NULL,
  processed_file smallint
);