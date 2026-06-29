import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=15432,
    database='de',
    user='jovyan',
    password='jovyan'
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
)
""")

conn.commit()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'stg' AND table_name LIKE 'ordersystem%'")
print("Tables after creation:", [r[0] for r in cur.fetchall()])
conn.close()