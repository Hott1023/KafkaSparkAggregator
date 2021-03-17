from pyspark.sql.types import *

EVENT_SCHEMA = StructType([StructField("user_id", IntegerType()),
                           StructField("request_status", StringType()),
                           StructField("response_time", StringType()),
                           StructField("end_point", StringType()),
                           StructField("server_name", StringType())])

EVENT_TOPIC = "kafka_event_topic"


class EventClass:
    def __init__(self, user_id, request_status, response_time, end_point, server_name):
        self.user_id = user_id
        self.request_status = request_status
        self.response_time = response_time
        self.end_point = end_point
        self.server_name = server_name

    def get_dict(self):
        return {'user_id': self.user_id,
                'request_status': self.request_status,
                'response_time': self.response_time,
                'end_point': self.end_point,
                'server_name': self.server_name}


def event_data_generator():
    return EventClass(0, 200, 1, '127.0.0.1', 'test').get_dict()
