from pyspark.sql.types import *

CLIENT_SESSION_SCHEMA = StructType([StructField("user_id", IntegerType()),
                                    StructField("user_platform", StringType()),
                                    StructField("session_duration", IntegerType()),
                                    StructField("user_ip_address", StringType())])

CLIENT_SESSION_TOPIC = "kafka_client_session_topic"


class ClientSessionClass:
    def __init__(self, user_id, user_platform, session_duration, user_ip_address):
        self.user_id = user_id
        self.user_platform = user_platform
        self.session_duration = session_duration
        self.user_ip_address = user_ip_address

    def get_dict(self):
        return {'user_id': self.user_id,
                'user_platform': self.user_platform,
                'session_duration': self.session_duration,
                'user_ip_address': self.user_ip_address}


def client_session_data_generator():
    return ClientSessionClass(0, 'user_platform', 10, '127.0.0.1').get_dict()
