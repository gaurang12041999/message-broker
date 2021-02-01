import pika
import pika_pool


# Single connection
class MessageBrokerConnector:
    @staticmethod
    def connect(authenticator):
        pass

    @staticmethod
    def close(connection):
        pass


class RabbitMqConnector(MessageBrokerConnector):
    @staticmethod
    def connect(authenticator):
        parameters = pika.URLParameters(authenticator.config['RABBIT_MQ_URL_PARAMETER'])
        connection = pika.BlockingConnection(parameters)
        return connection

    @staticmethod
    def close(connection):
        if connection is not None:
            connection.close()


class ConnectMessageBroker(object):
    def __init__(self, authenticator):
        self.conn = None
        self.authenticator = authenticator
        self.message_broker = authenticator.config['MESSAGE_BROKER']

    def __enter__(self):
        message_broker = ConnectMessageBroker.get_message_broker_handler(self.message_broker)
        self.conn = message_broker.connect(self.authenticator)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        message_broker = ConnectMessageBroker.get_message_broker_handler(self.message_broker)
        message_broker.close(self.conn)

    @staticmethod
    def get_message_broker_handler(message_broker):
        return {
            "rabbit_mq": RabbitMqConnector
        }.get(message_broker)


# Pool connection
class MessageBrokerPoolConnector:
    @staticmethod
    def connect(authenticator):
        pass


class RabbitMqPoolConnector(MessageBrokerPoolConnector):
    @staticmethod
    def connect(authenticator):
        params = pika.URLParameters(authenticator.config['RABBIT_MQ_URL_PARAMETER_PIKA_POOL'])
        return pika_pool.QueuedPool(
            create=lambda: pika.BlockingConnection(parameters=params),
            max_size=authenticator.config['POOL_MAXIMUM_CONNECTION_LIMIT'],
            max_overflow=authenticator.config['POOL_MAXIMUM_CONNECTION_OVERFLOW_LIMIT'],
            timeout=authenticator.config['POOL_MAXIMUM_CONNECTION_ACQUIRE_TIME'],
            stale=authenticator.config['POOL_CONNECTION_STATE_TIME_DURATION']
        )


class MessageBrokerPoolConnection:
    @staticmethod
    def get_message_broker_handler(message_broker):
        return {
            "rabbit_mq": RabbitMqPoolConnector
        }.get(message_broker)

    @staticmethod
    def create_connection_pool(authenticator):
        message_broker = MessageBrokerPoolConnection.get_message_broker_handler(authenticator.config['MESSAGE_BROKER'])
        return message_broker.connect(authenticator)
