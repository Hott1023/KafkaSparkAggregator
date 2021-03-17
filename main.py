from client_session_class import CLIENT_SESSION_TOPIC, client_session_data_generator
from event_class import EVENT_TOPIC, event_data_generator

import json
import threading
import time
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from kafka_producer import KafkaGenerator
from spark_region_server import SparkRegionServer
from spark_main_server import SparkMainServer


BOOTSTRAP_SERVER = 'localhost:9092'

if __name__ == '__main__':
    client_session_threads = KafkaGenerator(topic_name=CLIENT_SESSION_TOPIC,
                                            bootstrap_server=BOOTSTRAP_SERVER,
                                            data_generator=client_session_data_generator,
                                            sends_per_minute_for_thread=60,
                                            working_time=1).start()
    event_threads = KafkaGenerator(topic_name=EVENT_TOPIC,
                                   bootstrap_server=BOOTSTRAP_SERVER,
                                   data_generator=event_data_generator,
                                   sends_per_minute_for_thread=60,
                                   working_time=1).start()

    spark_region_server = SparkRegionServer(input_kafka_bootstrap_server=BOOTSTRAP_SERVER,
                                            output_kafka_bootstrap_server=BOOTSTRAP_SERVER)
    # spark_region_server.start()

    # spark_main_server = SparkMainServer(input_kafka_bootstrap_server=BOOTSTRAP_SERVER)
    # spark_main_server.start()

