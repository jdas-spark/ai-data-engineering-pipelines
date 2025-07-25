import snowflake.connector
import pandas as pd

conn = snowflake.connector.connect(
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='YOUR_WAREHOUSE',
    database='MY_DB',
    schema='MY_SCHEMA'
)

df = pd.read_csv('/tmp/transformed_sales.csv')
cursor = conn.cursor()
cursor.execute("PUT file:///tmp/transformed_sales.csv @%my_table")
cursor.execute("""
    COPY INTO my_table
    FROM @%my_table
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
""")
