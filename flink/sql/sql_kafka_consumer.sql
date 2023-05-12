-- flink-1.17.0/bin/sql-client.sh
-- make sure to add flink-sql-connector-kafka-1.17.0.jar dependecy to (flink-1.17.0/lib/) 
-- OR manually in SQL CLI : ADD JAR '/path/flink-sql-connector-kafka-1.17.0.jar';
CREATE TABLE events (
    name string,
    age int,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'quickstart-events',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

SELECT * FROM events;


SELECT SUM(age) as agSum, name FROM events GROUP BY name;