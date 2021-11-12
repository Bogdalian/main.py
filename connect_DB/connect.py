import psycopg2

DB = 'openspb'
USER = 'mmironov'
PASS = 'bxmu7jda7Hzm'
HOST = '10.241.0.197'
PORT = '5432'

conn = psycopg2.connect(database=DB,
                        user=USER,
                        password=PASS,
                        host=HOST,
                        port=PORT)