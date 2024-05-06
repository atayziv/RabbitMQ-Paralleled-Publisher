""""RabbitMQ Client."""

import json
import logging
from typing import Any, Dict

import pika

from textlabel_watchdog.clients.message_generator import ABCGeneratorService


class RabbitMQClient:
    """ "RabbitMQ Class."""

    def __init__(
        self,
        queues_connection_config: Dict[str, Any],
        queue_name: str,
        abs_message_generator: ABCGeneratorService,
    ) -> None:
        self.__logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._abs_message_generator = abs_message_generator
        self.__queue_name = queue_name
        self.__exchange = queues_connection_config.get("exchange")
        self._connection = None
        self._channel = None

        self._deliveries = {}
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self._stopping = False
        self.properties = pika.BasicProperties()
        """
        Initialize RabbitMQClient.

        :param queues_connection_config: Configuration for connecting to RabbitMQ.
        :param queue_name: Name of the queue.
        """
        credentials = pika.PlainCredentials(
            queues_connection_config.get("username"),
            queues_connection_config.get("password"),
        )
        parameters = pika.ConnectionParameters(
            host=queues_connection_config.get("hostname"),
            port=queues_connection_config.get("port"),
            virtual_host=queues_connection_config.get("virtual_host"),
            credentials=credentials,
        )
        self._connection = pika.SelectConnection(
            parameters=parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        self.__logger.info("Connection opened, creating a new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self.__logger.info("Channel opened, adding channel close callback")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.start_publishing()

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        self.__logger.warning("Channel %i was closed: %s", channel, reason)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.

        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error

        """
        self.__logger.error("Connection open failed, reopening in 5 seconds: %s", err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            self.__logger.warning("Connection closed, reopening in 5 seconds: %s", reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split(".")[1].lower()
        ack_multiple = method_frame.method.multiple
        delivery_tag = method_frame.method.delivery_tag

        self.__logger.info(
            "Received %s for delivery tag: %i (multiple: %s)",
            confirmation_type,
            delivery_tag,
            ack_multiple,
        )

        if confirmation_type == "ack":
            self._acked += 1
        elif confirmation_type == "nack":
            self._nacked += 1

        del self._deliveries[delivery_tag]

        if ack_multiple:
            for tmp_tag in list(self._deliveries.keys()):
                if tmp_tag <= delivery_tag:
                    self._acked += 1
                    del self._deliveries[tmp_tag]
        """
        NOTE: at some point you would check self._deliveries for stale
        entries and decide to attempt re-delivery
        """

        self.__logger.info(
            "Published %i messages, %i have yet to be confirmed, "
            "%i were acked and %i were nacked",
            self._message_number,
            len(self._deliveries),
            self._acked,
            self._nacked,
        )

    def initalize_async_publishing(self, data: Dict[str, Any], json_path: str) -> None:
        """
        Enqueue a message to RabbitMQ.

        :param queue_item: Item to enqueue.
        :return: Correlation ID of the enqueued message.
        """
        self.case_name = data["case_name"]
        self.paragraphs_data = data["data"]
        self.json_path = json_path
        self._connection.ioloop.start()

    def start_publishing(self):
        self._channel.confirm_delivery(self.on_delivery_confirmation)
        self.schedule_next_message()

    def schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """
        self._connection.ioloop.add_callback_threadsafe(self.publish_message)

    def publish_message(self):
        try:
            if self._channel is None or not self._channel.is_open:
                return
            message = self._abs_message_generator._generate_payload(
                self.paragraphs_data[self._message_number], self.json_path, self.case_name
            )

            self._channel.basic_publish(
                self.__exchange,
                self.__queue_name,
                json.dumps(message),
                self.properties,
            )
            self._message_number += 1
            self._deliveries[self._message_number] = True
            self.__logger.info("Published message # %i", self._message_number)
            self.schedule_next_message()
        except Exception as e:
            self.__logger.error(e)
