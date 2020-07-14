import configparser
import psycopg2

config = configparser.ConfigParer()
config.read('../config.cfg')

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

with conn.cursor() as cur:
    cur.execute(open("../create_tables.sql", "r").read())
conn.commit()
