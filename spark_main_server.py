from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import from_json
from pyspark.sql.functions import expr
from pyspark.sql.functions import col

DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss'


class SparkMainServer:

    def __init__(self,
                 input_kafka_bootstrap_server=None):
        if input_kafka_bootstrap_server is None:
            raise ValueError("'input_kafka_bootstrap_server' is None")

        self.spark = SparkSession \
            .builder \
            .appName("SparkMainServer") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel('WARN')

        self.kafka_loader = None

        self.input_kafka_bootstrap_server = input_kafka_bootstrap_server

        self.geographic_distribution_kafka_topic = f'geographic_distribution'
        self.platform_distribution_kafka_topic = f'platform_distribution'
        self.platform_city_dependency_kafka_topic = f'platform_city_dependency'
        self.prepared_session_duration_info_kafka_topic = f'prepared_session_duration_info'
        self.prepared_response_time_info_kafka_topic = f'prepared_response_time_info'

        self.geographic_distribution_data_frame = self.init_geographic_distribution_loader()
        self.platform_distribution_data_frame = self.init_platform_distribution_loader()
        self.platform_city_dependency_data_frame = self.init_platform_city_dependency_loader()
        self.prepared_session_duration_info_data_frame = self.init_prepared_session_duration_info_loader()
        self.prepared_response_time_info_data_frame = self.init_prepared_response_time_info_loader()

    def start(self):
        self.upload_data_frame_to_console(self.collect_geographic_distribution())
        self.upload_data_frame_to_console(self.collect_platform_distribution())
        self.upload_data_frame_to_console(self.collect_platform_city_dependency())
        self.upload_data_frame_to_console(self.collect_prepared_session_duration_info_with_calc_percentile())
        self.upload_data_frame_to_console(self.collect_prepared_response_time_info_with_calc_percentile())

    def init_kafka_loader(self, kafka_topic, kafka_value_schema):
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.input_kafka_bootstrap_server) \
            .option("subscribe", kafka_topic) \
            .load() \
            .select(from_unixtime(col('timestamp').cast('string'), format=DATE_FORMAT)
                    .cast('timestamp').alias('timestamp'),
                    from_json(col('value').cast('string'), kafka_value_schema).alias('parsed_json'))

    def init_geographic_distribution_loader(self):
        geographic_distribution_schema = StructType([StructField("start", StringType()),
                                                     StructField("end", StringType()),
                                                     StructField("city", StringType()),
                                                     StructField("count", IntegerType())])

        geographic_distribution_df = self.init_kafka_loader(self.geographic_distribution_kafka_topic,
                                                            geographic_distribution_schema)
        return geographic_distribution_df.select(
            geographic_distribution_df.parsed_json.getField('start').alias('start'),
            geographic_distribution_df.parsed_json.getField('end').alias('end'),
            geographic_distribution_df.parsed_json.getField('city').alias('city'),
            geographic_distribution_df.parsed_json.getField('count').alias('count'))

    def init_platform_distribution_loader(self):
        platform_distribution_schema = StructType([StructField("start", StringType()),
                                                   StructField("end", StringType()),
                                                   StructField("user_platform", StringType()),
                                                   StructField("count", IntegerType())])

        platform_distribution_df = self.init_kafka_loader(self.platform_distribution_kafka_topic,
                                                          platform_distribution_schema)
        return platform_distribution_df.select(
            platform_distribution_df.parsed_json.getField('start').alias('start'),
            platform_distribution_df.parsed_json.getField('end').alias('end'),
            platform_distribution_df.parsed_json.getField('user_platform').alias('user_platform'),
            platform_distribution_df.parsed_json.getField('count').alias('count'))

    def init_platform_city_dependency_loader(self):
        platform_city_dependency_schema = StructType([StructField("start", StringType()),
                                                      StructField("end", StringType()),
                                                      StructField("user_platform", StringType()),
                                                      StructField("city", StringType()),
                                                      StructField("count", IntegerType())])

        platform_city_dependency_df = self.init_kafka_loader(self.platform_city_dependency_kafka_topic,
                                                             platform_city_dependency_schema)
        return platform_city_dependency_df.select(
            platform_city_dependency_df.parsed_json.getField('start').alias('start'),
            platform_city_dependency_df.parsed_json.getField('end').alias('end'),
            platform_city_dependency_df.parsed_json.getField('city').alias('city'),
            platform_city_dependency_df.parsed_json.getField('user_platform').alias('user_platform'),
            platform_city_dependency_df.parsed_json.getField('count').alias('count'))

    def init_prepared_session_duration_info_loader(self):
        prepared_session_duration_info_schema = StructType([StructField("start", StringType()),
                                                            StructField("end", StringType()),
                                                            StructField("session_duration", IntegerType())])

        prepared_session_duration_info_df = self.init_kafka_loader(self.prepared_session_duration_info_kafka_topic,
                                                                   prepared_session_duration_info_schema)
        return prepared_session_duration_info_df.select(
            prepared_session_duration_info_df.parsed_json.getField('start').alias('start'),
            prepared_session_duration_info_df.parsed_json.getField('end').alias('end'),
            prepared_session_duration_info_df.parsed_json.getField('session_duration').alias('session_duration'))

    def init_prepared_response_time_info_loader(self):
        prepared_response_time_info_schema = StructType([StructField("start", StringType()),
                                                         StructField("end", StringType()),
                                                         StructField("response_time", IntegerType())])

        prepared_response_time_info_df = self.init_kafka_loader(self.prepared_response_time_info_kafka_topic,
                                                                prepared_response_time_info_schema)
        return prepared_response_time_info_df.select(
            prepared_response_time_info_df.parsed_json.getField('start').alias('start'),
            prepared_response_time_info_df.parsed_json.getField('end').alias('end'),
            prepared_response_time_info_df.parsed_json.getField('response_time').alias('response_time'))

    def collect_geographic_distribution(self):
        return self.geographic_distribution_data_frame \
            .groupBy('start', 'end', 'city') \
            .sum() \
            .select(col('start'), col('end'), col('city'), col('sum(count)').alias('sum'))

    def collect_platform_distribution(self):
        return self.platform_distribution_data_frame \
            .groupBy('start', 'end', 'user_platform') \
            .sum() \
            .select(col('start'), col('end'), col('user_platform'), col('sum(count)').alias('sum'))

    def collect_platform_city_dependency(self):
        return self.platform_city_dependency_data_frame \
            .groupBy('start', 'end', 'user_platform', 'city') \
            .sum() \
            .select(col('start'), col('end'), col('user_platform'), col('city'), col('sum(count)').alias('sum'))

    def collect_prepared_session_duration_info_with_calc_percentile(self):
        return self.prepared_session_duration_info_data_frame \
            .groupBy('start', 'end') \
            .agg(expr("approx_percentile(session_duration, array(0.5))").alias("percentile")) \
            .select(col('start'), col('end'), col('percentile'))

    def collect_prepared_response_time_info_with_calc_percentile(self):
        return self.prepared_response_time_info_data_frame \
            .groupBy('start', 'end') \
            .agg(expr("approx_percentile(response_time, array(0.5))").alias("percentile")) \
            .select(col('start'), col('end'), col('percentile'))

    @staticmethod
    def upload_data_frame_to_console(data_frame):
        data_frame.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

    @staticmethod
    def upload_data_frame_to_json(data_frame):
        data_frame.writeStream \
            .format("json") \
            .option("path", "json-data") \
            .start()
