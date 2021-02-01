from WotNotMessageBroker.connect_message_broker import ConnectMessageBroker


class MessageBrokerQueuesCreator:
    @staticmethod
    def create_queues(**kwargs):
        pass


class CreateRabbitMqQueses(MessageBrokerQueuesCreator):
    @staticmethod
    def create_queues(**kwargs):
         with ConnectMessageBroker(kwargs.get("authenticator")) as conn:
            for data in kwargs.get("queues"):
                channel = conn.channel()
                channel.exchange_declare(exchange=data.get('exchange_name'),
                                         exchange_type=data.get('exchange_type'), durable=True)
                channel.queue_declare(queue=data.get('queue'), durable=True)
                channel.queue_bind(exchange=data.get('exchange_name'), routing_key=data.get('route'),
                                   queue=data.get('queue'))


class CreateQueses:
    @staticmethod
    def get_message_broker_handler(message_broker):
        return {
            "rabbit_mq": CreateRabbitMqQueses
        }.get(message_broker)

    @staticmethod
    def create_queues(authenticator, queues):
        message_broker = CreateQueses.get_message_broker_handler(authenticator.config['MESSAGE_BROKER'])
        message_broker.create_queues(authenticator=authenticator, queues=queues)
