import pika
import json


class MessageBrokerPublisher:
    @staticmethod
    def publish_message(**kwargs):
        pass

    @staticmethod
    def publish_message_in_bulk(**kwargs):
        pass


class RabbiMQPublisher(MessageBrokerPublisher):
    @staticmethod
    def publish_message(**kwargs):
        with kwargs.get("conn").acquire() as cxn:
            cxn.channel.exchange_declare(exchange=kwargs.get("exchange_name"), exchange_type=kwargs.get("exchange_type"),
                                         durable=True)
            cxn.channel.basic_publish(exchange=kwargs.get("exchange_name"),
                                      routing_key=kwargs.get("route_key"),
                                      body=json.dumps(kwargs.get("payload"), ensure_ascii=False),
                                      properties=pika.BasicProperties(delivery_mode=kwargs.get("delivery_mode"), ))

    @staticmethod
    def publish_message_in_bulk(**kwargs):
        with kwargs.get("conn").acquire() as cxn:
            cxn.channel.exchange_declare(exchange=kwargs.get("exchange_name"), exchange_type=kwargs.get("exchange_type"),
                                         durable=True)
            for data in kwargs.get("payload"):
                cxn.channel.basic_publish(exchange=kwargs.get("exchange_name"),
                                          routing_key=data['route_key'],
                                          body=json.dumps(data['payload'], ensure_ascii=False),
                                          properties=pika.BasicProperties(delivery_mode=kwargs.get("delivery_mode"), ))


class PublishMessage:
    @staticmethod
    def get_message_broker_handler(message_broker):
        return {
            "rabbit_mq": RabbiMQPublisher
        }.get(message_broker)

    @staticmethod
    def publish_message(authenticator, route_key, payload, exchange_type="direct", delivery_mode=2,
                        exchange_name="wotnot.direct"):
        message_broker = PublishMessage.get_message_broker_handler(authenticator.config['MESSAGE_BROKER'])
        message_broker.publish_message(conn=authenticator.connection_pool, route_key=route_key, payload=payload, exchange_name=exchange_name,
                                       exchange_type=exchange_type, delivery_mode=delivery_mode)

    @staticmethod
    def publish_message_in_bulk(authenticator, payload, exchange_type="direct", delivery_mode=2,
                                exchange_name="wotnot.direct",):
        message_broker = PublishMessage.get_message_broker_handler(authenticator.config['MESSAGE_BROKER'])
        message_broker.publish_message_in_bulk(conn=authenticator.connection_pool, payload=payload, exchange_type=exchange_type,
                                       delivery_mode=delivery_mode, exchange_name=exchange_name)
