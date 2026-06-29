import psycopg2
import os

conn = psycopg2.connect(
    host='localhost',
    port=15432,
    database='de',
    user='jovyan',
    password='jovyan'
)
cur = conn.cursor()
cur.execute("SELECT current_database(), inet_server_addr(), inet_server_port()")
print("DB, addr, port:", cur.fetchone())
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'stg' ORDER BY table_name")
print("Tables:", [r[0] for r in cur.fetchall()])
conn.close()