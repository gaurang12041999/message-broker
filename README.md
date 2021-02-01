This package will help to perform Rabbit MQ operations


# Install

You can install it with pip:


`pip install git+https://github.com/gaurang19990412/message-broker`


# Example code

```
from WotNotMessageBroker.utility.authentication import initialize_authentication_object
from WotNotMessageBroker.create_queue import CreateQueses
from WotNotMessageBroker.pusblish_message import PublishMessage


authenticator = initialize_authentication_object(MESSAGE_BROKER="rabbit_mq",
                                                 MESSAGE_BROKER_HOST="localhost",
                                                 MESSAGE_BROKER_USERNAME="guest",
                                                 MESSAGE_BROKER_PASSWORD="guest")


# Create queue
queues = [{'route': 'message_broker_queue_1',
           'queue': 'message_broker_queue_1_q',
           'worker_count': 2,
           'exchange_type': 'direct',
           'exchange_name': 'wotnot.direct'},
          {'route': 'message_broker_queue_2',
           'queue': 'message_broker_queue_2_q',
           'worker_count': 2,
           'exchange_type': 'direct',
           'exchange_name': 'wotnot.direct'}
          ]
CreateQueses.create_queues(authenticator, queues)

# Publish message
PublishMessage.publish_message(authenticator=authenticator,
                               route_key="message_broker_queue_1",
                               payload={"Ok": "This is first message"}, exchange_type="direct", delivery_mode=2,
                               exchange_name="wotnot.direct")

PublishMessage.publish_message_in_bulk(authenticator=authenticator,
                                       payload=[{"payload": {"Ok": "This is first message"}, "route_key": "message_broker_queue_2"},
                                                {"payload": {"Ok": "This is second message"}, "route_key": "message_broker_queue_2"}],
                                       exchange_type="direct", delivery_mode=2,
                                       exchange_name="wotnot.direct")

```