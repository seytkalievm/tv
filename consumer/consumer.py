from __future__ import (
    absolute_import,
    division,
    print_function,
)
from functools import partial
import pika
import os
import psycopg2

EXCHANGE = 'exchange'
QUEUE = 'exchange.receiver'

def main():
    """Main entry point to the program."""

    username = os.environ['USERNAME']
    password = os.environ['PASSWORD']

    credentials = pika.PlainCredentials(username=username, password=password)
    parameters = pika.ConnectionParameters(host='rabbitMQ',
                                   port=5672,
                                   virtual_host= '/',
                                   credentials=credentials)
    connection = pika.SelectConnection(parameters, on_open_callback=on_open)

    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()


def on_open(connection):
    """Callback when we have connected to the AMQP broker."""
    print('Connected')
    connection.channel(on_channel_open)


def on_channel_open(channel):
    """Callback when we have opened a channel on the connection."""
    print('Have channel')

    channel.exchange_declare(exchange=EXCHANGE, exchange_type='fanout',
                             durable=True,
                             callback=partial(on_exchange, channel))


def on_exchange(channel, frame):
    """Callback when we have successfully declared the exchange."""
    print('Have exchange')
    channel.queue_declare(queue=QUEUE, durable=True,
                          callback=partial(on_queue, channel))


def on_queue(channel, frame):
    """Callback when we have successfully declared the queue."""
    print('Have queue')

    channel.basic_qos(prefetch_count=1, callback=partial(on_qos, channel))


def on_qos(channel, frame):
    """Callback when we have set the channel prefetch limit."""
    print('Set QoS')
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE,
                       callback=partial(on_bind, channel))


def on_bind(channel, frame):
    """Callback when we have successfully bound the queue to the exchange."""
    print('Bound')
    channel.basic_consume(queue=QUEUE, consumer_callback=on_message)


def on_message(channel, delivery, properties, body):
    """Callback when a message arrives.
    :param channel: the AMQP channel object.
    :type channel: :class:`pika.channel.Channel`
    :param delivery: the AMQP protocol-level delivery object,
      which includes a tag, the exchange name, and the routing key.
      All of this should be information the sender has as well.
    :type delivery: :class:`pika.spec.Deliver`
    :param properties: AMQP per-message metadata.  This includes
      things like the body's content type, the correlation ID and
      reply-to queue for RPC-style messaging, a message ID, and so
      on.  It also includes an additional table of structured
      caller-provided headers.  Again, all of this is information
      the sender provided as part of the message.
    :type properties: :class:`pika.spec.BasicProperties`
    :param str body: Byte string of the message body.
    """
    print('Exchange: %s' % (delivery.exchange,))
    print('Routing key: %s' % (delivery.routing_key,))
    print('Content type: %s' % (properties.content_type,))
    print()
    print(body)
    print()

    on_submit(body)

    channel.basic_ack(delivery.delivery_tag)

def on_submit(message):
    """Push to PostgreSQL"""

    message = message.split(',')
    conn = psycopg2.connect(
    database=os.environ(POSTGRES_DB), user=os.environ(POSTGRES_USER), password=os.environ(POSTGRES_PASSWORD), host='127.0.0.1', port= '5432'
    )
    conn.autocommit = True

    cursor = conn.cursor()
    if message[0] == "Actors":
        cursor.execute(f'''INSERT INTO Courses("Full_name", "Last_film", "Raiting") VALUES ({message[1]}, {message[2]}, {message[3]})''')
    if message[0] == "Singers":
        cursor.execute(f'''INSERT INTO Clients("Full_name", "Last_song", "Raiting") VALUES ({message[1]}, {message[2]}, {message[3]})''')

    conn.commit()
    print("Record inserted to the table")

    conn.close()

if __name__ == '__main__':
    main()