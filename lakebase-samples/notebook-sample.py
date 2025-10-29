# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import uuid

w = WorkspaceClient()
instance_name = "<lakebase-instance-name>"
instance = w.database.get_database_instance(name=instance_name)
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])

# COMMAND ----------

# MAGIC %pip install sqlalchemy==1.4 psycopg[binary]

# COMMAND ----------

from sqlalchemy import create_engine, text

from databricks.sdk import WorkspaceClient
import uuid

user = "<user-name>"
host = instance.read_write_dns
port = 5432
database = "<postgres-database-name>"
password = cred.token

connection_pool = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require")

with connection_pool.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    for row in result:
        print(f"Connected to PostgreSQL database. Version: {row}")