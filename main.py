from clickhouse_driver import Client
import pandas as pd
import os

clickhouse_url = "http://localhost:8123/"

client = Client(
    host='localhost'
)

query = """
CREATE DATABASE IF NOT EXISTS myClickHouse;
"""
client.execute(query)

print("Database myClickHouse created successfully.")
print('------------------------------------------------------------------------------------------------')

query = f"""
CREATE TABLE IF NOT EXISTS myClickHouse.advertising_costs (
  date Date,
  app_id String,
  platform String,
  source String,
  views Int64,
  cost Float64
) ENGINE MergeTree() ORDER BY (date, app_id, platform, source);
"""
client.execute(query)
print("Table advertising_costs created successfully.")


query = f"""
CREATE TABLE IF NOT EXISTS myClickHouse.user_activity
(
    user_id String,
    event_timestamp DateTime,
    event_type String
) ENGINE = MergeTree()
ORDER BY (user_id, event_timestamp);
"""
client.execute(query)
print("Table user_activity created successfully.")



query = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS myClickHouse.user_activity_sessions 
ENGINE = MergeTree() 
ORDER BY (user_id, session_id) 
AS
SELECT
    user_id,
    event_timestamp,
    event_type,
    intDiv(toUInt32(event_timestamp), 900) AS session_id
FROM
    myClickHouse.user_activity
ORDER BY user_id, event_timestamp;
"""
client.execute(query)
print("Materialized view user_activity_sessions created successfully.")
print("Tables and view created successfully!")

query = """
ALTER TABLE IF EXISTS myClickHouse.advertising_costs
ADD COLUMN IF NOT EXISTS version UInt32 DEFAULT 1;
"""
client.execute(query)

query = """
ALTER TABLE IF EXISTS myClickHouse.user_activity
ADD COLUMN IF NOT EXISTS version UInt32 DEFAULT 1;
"""
client.execute(query)

directory_path = "data/"
batch_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith(".csv")]

for batch_file in batch_files:
    with open(batch_file, 'r') as f:
        df = pd.read_csv(f)
        
        for index, row in df.iterrows():
            date, app_id, platform, source, views, cost = row
            
            query = f"""
            INSERT INTO myClickHouse.advertising_costs (date, app_id, platform, source, views, cost, version)
            VALUES ('{date}', '{app_id}', '{platform}', '{source}', {views}, {cost}, 1);
            ON DUPLICATE KEY UPDATE
                views = views + VALUES(views),
                cost = VALUES(cost),
                version = version + 1;
            """
            
            client.execute(query)

print("Data successfully inserted into advertising_costs table.")


csv_file_path = "dataForTask2/test_task_2.csv"

with open(csv_file_path, 'r') as f:
    next(f) 
    for line in f:
        user_id, event_timestamp, event_type = line.strip().split(',')
        
        insert_data_query = f"""
        INSERT INTO myClickHouse.user_activity (user_id, event_timestamp, event_type, version)
        VALUES ('{user_id}', '{event_timestamp}', '{event_type}', 1);
        ON DUPLICATE KEY UPDATE
            event_timestamp = GREATEST(event_timestamp, VALUES(event_timestamp)),
            event_type = VALUES(event_type),
            version = version + 1;
        """
    
        client.execute(insert_data_query)

print("Manual data insertion into user_activity table complete.")
print("Script execution completed.")
