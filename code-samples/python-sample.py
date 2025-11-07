
import psycopg2

# Generate token through the Lakebase UI, REST API, or using the Databricks SDK
PGPASSWORD = "<OAUTH-token>"
conn_string = f"<conn-string-from-lakebase-UI>"

conn = psycopg2.connect(conn_string)
with conn.cursor() as cur:
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(version)
conn.close()