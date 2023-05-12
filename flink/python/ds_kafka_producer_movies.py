from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema

import json
file_path = '/home/matar/real-time-data-processing-demo/flink/etc/async_movies_full.json'


env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:////home/matar/real-time-data-processing-demo/flink/etc/flink-sql-connector-kafka-1.17.0.jar")

row_type_info = Types.ROW_NAMED(["movie_id","movie_url","name","year","rating","desc","poster"], 
                                [Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(),
                                 Types.INT(), Types.STRING(), Types.STRING()])

json_format = JsonRowSerializationSchema.builder().with_type_info(row_type_info).build()

serialization_schema = KafkaRecordSerializationSchema.builder() \
                        .set_topic("movies") \
                        .set_value_serialization_schema(json_format) \
                        .build()

sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(serialization_schema) \
        .build()

with open(file_path, 'r') as mv:
     movies = mv.read()
parese_movies_json = json.loads(movies)

for i in range(len(parese_movies_json)):
       ds = env.from_collection([tuple(parese_movies_json[i].values())], row_type_info)
       ds.sink_to(sink)
       env.execute()

# def parse_jons(file_path) :
#     with open(file_path, 'r') as mv:
#         movies = mv.read()
#     parese_movies_json = json.loads(movies)
#     for i in len(parese_movies_json):
#         yield json.dumps(parese_movies_json[i])