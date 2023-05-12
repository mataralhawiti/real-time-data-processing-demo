from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema


env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:////home/matar/real-time-data-processing-demo/flink/etc/flink-sql-connector-kafka-1.17.0.jar")

row_type_info = Types.ROW_NAMED(['name', 'age'], [Types.STRING(), Types.INT()])

ds = env.from_collection([("Matar", 15000)], row_type_info)

json_format = JsonRowSerializationSchema.builder().with_type_info(row_type_info).build()

serialization_schema = KafkaRecordSerializationSchema.builder() \
                        .set_topic("quickstart-events") \
                        .set_value_serialization_schema(json_format) \
                        .build()

sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(serialization_schema) \
        .build()

ds.sink_to(sink)
env.execute()