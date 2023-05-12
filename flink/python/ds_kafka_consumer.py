from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:////home/matar/real-time-data-processing-demo/flink/etc/flink-sql-connector-kafka-1.17.0.jar")


# Reading from Kafka
####################
row_type_info = Types.ROW_NAMED(['name', 'age'], [Types.STRING(), Types.INT()])
deserialization_schema = JsonRowDeserializationSchema.builder().type_info(row_type_info).build()

source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("quickstart-events") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_group_id("fun") \
        .set_value_only_deserializer(deserialization_schema) \
        .build()
ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka source").print()

env.execute()