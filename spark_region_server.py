from client_session_class import CLIENT_SESSION_SCHEMA, CLIENT_SESSION_TOPIC
from event_class import EVENT_SCHEMA, EVENT_TOPIC

from lib.ip_adress_map import prepare_ip_address_map
from lib.ip_adress_map import IP_ADDRESS_MAP

from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_json
from pyspark.sql.functions import to_json
from pyspark.sql.functions import struct
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import window
from pyspark.sql.functions import floor
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

KAFKA_DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss.SSS'
DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss'

CHECKPOINT_LOCATION = "checkpoint"

AVAILABLE_WINDOWS = ['1 minute',
                     '5 minutes',
                     '10 minutes',
                     '15 minutes',
                     '60 minutes',
                     '240 minutes']


class SparkRegionServer:

    def __init__(self,
                 input_kafka_bootstrap_server=None,
                 output_kafka_bootstrap_server=None):

        if input_kafka_bootstrap_server is None:
            raise ValueError("'input_kafka_bootstrap_server' is None")
        if output_kafka_bootstrap_server is None:
            raise ValueError("'output_kafka_bootstrap_server' is None")

        self.spark = SparkSession \
            .builder \
            .appName("SparkRegionServer") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel('WARN')

        self.input_kafka_bootstrap_server = input_kafka_bootstrap_server
        self.output_kafka_bootstrap_server = output_kafka_bootstrap_server

        session_df = self.init_kafka_loader(CLIENT_SESSION_TOPIC, CLIENT_SESSION_SCHEMA)
        self.client_session_data_frame = session_df.select(
            session_df.timestamp.alias('timestamp'),
            session_df.parsed_json.getField('user_id').alias('user_id'),
            session_df.parsed_json.getField('user_platform').alias('user_platform'),
            session_df.parsed_json.getField('session_duration').alias('session_duration'),
            session_df.parsed_json.getField('user_ip_address').alias('user_ip_address'))

        self.client_session_data_frame = self.city_by_ip_mapping_client_session()

        self.upload_data_frame_to_console(self.client_session_data_frame)
        # self.upload_data_frame_to_console(self.client_session_data_frame
        #                                   # .withWatermark("timestamp", '10 seconds')
        #                                   .groupBy(window(col('timestamp'), '10 seconds'), 'city')
        #                                   .count())

        return

        event_df = self.init_kafka_loader(EVENT_TOPIC, EVENT_SCHEMA)
        self.event_data_frame = event_df.select(
            event_df.timestamp,
            event_df.parsed_json.getField('user_id').alias('user_id'),
            event_df.parsed_json.getField('request_status').alias('request_status'),
            event_df.parsed_json.getField('response_time').alias('response_time'),
            event_df.parsed_json.getField('end_point').alias('end_point'),
            event_df.parsed_json.getField('server_name').alias('server_name'))
        for windowed_data_frame in self.geographic_distribution():
            self.upload_data_frame_to_console(windowed_data_frame['data_frame'])

    def start(self):
        self.upload_windowed_data_frames(self.geographic_distribution())
        self.upload_windowed_data_frames(self.platform_distribution())
        self.upload_windowed_data_frames(self.platform_city_dependency())
        self.upload_windowed_data_frames(self.prepared_session_duration_info())
        self.upload_windowed_data_frames(self.prepared_response_time_info())

    def init_kafka_loader(self, kafka_topic, kafka_value_schema):
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.input_kafka_bootstrap_server) \
            .option("subscribe", kafka_topic) \
            .load() \
            .select(from_unixtime(unix_timestamp(col('timestamp'), KAFKA_DATE_FORMAT), format=DATE_FORMAT)
                    .alias('timestamp'),
                    from_json(col('value').cast('string'), kafka_value_schema).alias('parsed_json'))

    def generate_city_map_data_frame(self):
        return self.spark.createDataFrame(
            prepare_ip_address_map(IP_ADDRESS_MAP),
            schema=StructType([StructField("map", MapType(StringType(), StringType()))]))

    def city_by_ip_mapping_client_session(self):
        city_map_data_frame = self.generate_city_map_data_frame()
        return self.client_session_data_frame \
            .join(broadcast(city_map_data_frame
                            .select(explode("map").alias("user_ip_address", "city"))),
                  on='user_ip_address',
                  how='left')

    def geographic_distribution(self):
        windowed_data_frames = list()
        for window_duration in AVAILABLE_WINDOWS:
            topic = f'geographic_distribution'
            if window_duration == 'inf':
                data_frame = self.client_session_data_frame \
                    .groupBy('city') \
                    .count() \
                    .withColumn('start', lit(None)) \
                    .withColumn('end', lit(None))
            else:
                client_session_window = self.client_session_data_frame \
                    .withWatermark("timestamp", window_duration) \
                    .groupBy(window(col('timestamp'), window_duration), 'city') \
                    .count()
                data_frame = client_session_window \
                    .select(client_session_window.window.start.cast('string').alias('start'),
                            client_session_window.window.end.cast('string').alias('end'),
                            'city', 'count')
            windowed_data_frames.append({'data_frame': data_frame, 'topic': topic})
        return windowed_data_frames

    def platform_distribution(self):
        windowed_data_frames = list()
        for window_duration in AVAILABLE_WINDOWS:
            topic = f'platform_distribution'
            if window_duration == 'inf':
                data_frame = self.client_session_data_frame \
                    .groupBy('user_platform') \
                    .count() \
                    .withColumn('start', lit(None)) \
                    .withColumn('end', lit(None))
            else:
                client_session_window = self.client_session_data_frame \
                    .withWatermark("timestamp", window_duration) \
                    .groupBy(window('timestamp', window_duration), 'user_platform') \
                    .count()
                data_frame = client_session_window \
                    .select(client_session_window.window.start.cast('string').alias('start'),
                            client_session_window.window.end.cast('string').alias('end'),
                            'user_platform', 'count')
            windowed_data_frames.append({'data_frame': data_frame, 'topic': topic})
        return windowed_data_frames

    def platform_city_dependency(self):
        windowed_data_frames = list()
        for window_duration in AVAILABLE_WINDOWS:
            topic = f'platform_city_dependency'
            if window_duration == 'inf':
                data_frame = self.client_session_data_frame \
                    .groupBy('user_platform', 'city') \
                    .count() \
                    .withColumn('start', lit(None)) \
                    .withColumn('end', lit(None))
            else:
                client_session_window = self.client_session_data_frame \
                    .withWatermark("timestamp", window_duration) \
                    .groupBy(window('timestamp', window_duration), 'user_platform', 'city') \
                    .count()
                data_frame = client_session_window \
                    .select(client_session_window.window.start.cast('string').alias('start'),
                            client_session_window.window.end.cast('string').alias('end'),
                            'user_platform', 'city', 'count')
            windowed_data_frames.append({'data_frame': data_frame, 'topic': topic})
        return windowed_data_frames

    def get_bordered_session_time(self, max_value, min_value, interval_counts):
        interval_length = (max_value - min_value) / interval_counts
        return self.client_session_data_frame \
            .withColumn('session_duration_interval_num',
                        floor((col('session_duration') - lit(min_value)) / lit(interval_length))) \
            .when(col('session_duration_interval_num') >= lit(interval_counts), interval_counts) \
            .when(col('session_duration_interval_num') <= lit(0), 0) \
            .withColumn('session_duration_start_interval',
                        col('session_duration_interval_num') * lit(interval_length) + lit(min_value)) \
            .withColumn('session_duration_end_interval',
                        col('session_duration_start_interval') + lit(interval_length))

    def prepared_session_duration_info(self):
        windowed_data_frames = list()
        for window_duration in AVAILABLE_WINDOWS:
            topic = f'prepared_session_duration_info'
            if window_duration == 'inf':
                data_frame = self.client_session_data_frame \
                    .select(col('session_duration')) \
                    .withColumn('start', lit(None)) \
                    .withColumn('end', lit(None))
            else:
                data_frame = self.client_session_data_frame \
                    .withWatermark("timestamp", window_duration) \
                    .orderBy(window('timestamp', window_duration)) \
                    .select(col('session_duration'),
                            col('window').start.cast('string').alias('start'),
                            col('window').end.cast('string').alias('end'))
            windowed_data_frames.append({'data_frame': data_frame, 'topic': topic})
        return windowed_data_frames

    def prepared_response_time_info(self):
        windowed_data_frames = list()
        for window_duration in AVAILABLE_WINDOWS:
            topic = f'prepared_response_time_info'
            if window_duration == 'inf':
                data_frame = self.client_session_data_frame \
                    .select(col('response_time')) \
                    .withColumn('start', lit(None)) \
                    .withColumn('end', lit(None))
            else:
                data_frame = self.client_session_data_frame \
                    .orderBy(window('timestamp', window_duration)) \
                    .select(col('response_time'),
                            col('window').start.cast('string').alias('start'),
                            col('window').end.cast('string').alias('end'))
            windowed_data_frames.append({'data_frame': data_frame, 'topic': topic})
        return windowed_data_frames

    def upload_data_frame_to_kafka(self, data_frame, output_kafka_topic):
        data_frame.select(to_json(struct([col(c).alias(c) for c in data_frame.columns])).alias('value')) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.output_kafka_bootstrap_server) \
            .option("topic", output_kafka_topic) \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .start()

    def upload_windowed_data_frames(self, windowed_data_frames):
        for windowed_data_frame in windowed_data_frames:
            self.upload_data_frame_to_kafka(windowed_data_frame['data_frame'],
                                            windowed_data_frame['topic'])

    @staticmethod
    def upload_data_frame_to_console(data_frame):
        # .option("truncate", "false") \

        data_frame.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start() \
            .awaitTermination()
