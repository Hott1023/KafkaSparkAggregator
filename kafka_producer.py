import json
import threading
import time
from kafka import KafkaProducer


class KafkaGenerator:
    SECONDS_PER_MINUTE = 60

    def __init__(self,
                 topic_name=None,
                 bootstrap_server=None,
                 data_generator=None,
                 thread_count=1,
                 sends_per_minute_for_thread=1,
                 working_time=1):

        if topic_name is None:
            raise ValueError("'topic_name' is None")
        if bootstrap_server is None:
            raise ValueError("'bootstrap_server' is None")
        if data_generator is None:
            raise ValueError("'data_generator' is None")
        self.kafka_producer = None
        self.topic_name = topic_name
        self.bootstrap_server = bootstrap_server
        self.data_generator = data_generator
        self.thread_count = thread_count
        self.sends_per_minute_for_thread = sends_per_minute_for_thread
        self.working_time = working_time

    def get_thread_tuple(self):
        if self.kafka_producer is None:
            raise ValueError("'producer' is not defined")

        topic_name = self.topic_name
        producer = self.kafka_producer
        repeat_counts = self.working_time * self.sends_per_minute_for_thread
        sending_delay = 60 / self.sends_per_minute_for_thread
        data_generator = self.data_generator
        return topic_name, producer, repeat_counts, sending_delay, data_generator

    def init_kafka_producer(self):
        self.kafka_producer = KafkaProducer(bootstrap_servers=[self.bootstrap_server],
                                            value_serializer=lambda x:
                                            json.dumps(x).encode('utf-8'))

    def start(self):
        def data_sender_thread(topic_name, producer, repeat_counts, sending_delay, data_generator):
            for _ in range(repeat_counts):
                producer.send(topic_name, value=data_generator())
                time.sleep(sending_delay)

        self.init_kafka_producer()
        threads = list()
        for _ in range(self.thread_count):
            thread_tuple = self.get_thread_tuple()
            threads.append(
                threading.Thread(target=data_sender_thread,
                                 args=thread_tuple,
                                 daemon=False)
            )
            threads[-1].start()
        return threads
