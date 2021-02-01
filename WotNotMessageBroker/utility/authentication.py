from WotNotMessageBroker.utility.utils import Messenger
from WotNotMessageBroker.connect_message_broker import MessageBrokerPoolConnection

_AMQP_URL = 'amqp://{user_name}:{password}@{host}:{port}/{vhost}?heartbeat={heart_beat}&retry_delay=5&connection_attempts=3'


def initialize_authentication_object(**kwargs):
    """"This method is used to create authentication object which can be use while connecting message broker"""

    authenticator = Messenger()
    authenticator.config = create_config(**kwargs)
    create_message_broker_url(authenticator)
    create_pool_connection(authenticator)

    return authenticator


def create_config(**kwargs):
    """
        :param
            MESSAGE_BROKER: Message Broker which you want to use
            MESSAGE_BROKER_HOST: Address of host where Message Broker is running
            MESSAGE_BROKER_PORT: Port of Message Broker
            MESSAGE_BROKER_USERNAME: User name for Message Broker
            MESSAGE_BROKER_PASSWORD: Message Broker password associated with the given host

        :return Authentication object
    """
    return {
        "MESSAGE_BROKER": kwargs.get("MESSAGE_BROKER", "rabbit_mq"),
        "MESSAGE_BROKER_HOST": kwargs.get("MESSAGE_BROKER_HOST", "25.25.25.25"),
        "MESSAGE_BROKER_PORT": int(kwargs.get("MESSAGE_BROKER_PORT", 5672)),
        "MESSAGE_BROKER_USERNAME": kwargs.get("MESSAGE_BROKER_USERNAME", "XYZ"),
        "MESSAGE_BROKER_PASSWORD": kwargs.get("MESSAGE_BROKER_PASSWORD", "xyz"),
        "POOL_MAXIMUM_CONNECTION_LIMIT": int(kwargs.get("PIKA_POOL_MAXIMUM_CONNECTION_LIMIT", 1)),
        "POOL_MAXIMUM_CONNECTION_OVERFLOW_LIMIT": int(kwargs.get("PIKA_POOL_MAXIMUM_CONNECTION_OVERFLOW_LIMIT", 10)),
        "POOL_MAXIMUM_CONNECTION_ACQUIRE_TIME": int(kwargs.get("POOL_MAXIMUM_CONNECTION_ACQUIRE_TIME", 10)),
        "POOL_CONNECTION_STATE_TIME_DURATION": int(kwargs.get("POOL_CONNECTION_STATE_TIME_DURATION", 3600))
    }


def create_message_broker_url(authenticator):
    if authenticator.config['MESSAGE_BROKER'] == "rabbit_mq":
        authenticator.config['RABBIT_MQ_URL_PARAMETER'] = _AMQP_URL.format(
            user_name=authenticator.config['MESSAGE_BROKER_USERNAME'],
            password=authenticator.config['MESSAGE_BROKER_PASSWORD'],
            host=authenticator.config['MESSAGE_BROKER_HOST'],
            vhost="%2f",
            port=authenticator.config['MESSAGE_BROKER_PORT'],
            heart_beat=60)

        authenticator.config['RABBIT_MQ_URL_PARAMETER_PIKA_POOL'] = _AMQP_URL.format(
            user_name=authenticator.config['MESSAGE_BROKER_USERNAME'],
            password=authenticator.config['MESSAGE_BROKER_PASSWORD'],
            host=authenticator.config['MESSAGE_BROKER_HOST'],
            vhost="%2f",
            port=authenticator.config['MESSAGE_BROKER_PORT'],
            heart_beat=0)


def create_pool_connection(authenticator):
    authenticator.connection_pool = MessageBrokerPoolConnection.create_connection_pool(authenticator)
