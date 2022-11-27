from __future__ import (
    absolute_import,
    division,
    print_function,
)
from functools import partial
import pika
import os

EXCHANGE = 'exchange'
ROUTING_KEY = 'exchange.example'
DELAY = 5


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
    send_message(channel, 0)


def send_message(channel, i):
    """Send a message to the queue.
    This function also registers itself as a timeout function, so the
    main :mod:`pika` loop will call this function again every 5 seconds.
    """
    msg = 'Message %d' % (i,)
    print(msg)
    channel.basic_publish(EXCHANGE, ROUTING_KEY, msg,
                          pika.BasicProperties(content_type='text/plain',
                                               delivery_mode=2))
    channel.connection.add_timeout(DELAY,
                                   partial(send_message, channel, i+1))


# Python boilerplate to run the main function if this is run as a
# program.  You can 'import publisher' from other Python scripts in
# the same directory to get access to the functions here, or run
# 'pydoc publisher.py' to see the doc strings, and so on, but these
# require this call.
if __name__ == '__main__':
    main()