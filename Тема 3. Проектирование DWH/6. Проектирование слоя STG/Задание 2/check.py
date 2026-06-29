import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=15432,
    database='de',
    user='jovyan',
    password='jovyan'
)
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'stg' AND table_name LIKE 'ordersystem%'")
rows = cur.fetchall()
print("Таблицы в схеме stg:", [row[0] for row in rows])
conn.close()