"""
data_schemas.py

This module defines unified JSON data structures used across services.
It contains:

1. NotificationRecord – used for storing notifications (e.g., in Redis or a database table).
   Each notification record has a "type" field (which can be one of "message", "ack", "system", or "other")
   and a "body" field, which holds the payload details.

2. KafkaEventRecord – used for messages posted to Kafka.
   Each event record includes an "event_type" (e.g., "ack_chat" or "message")
   and a "body" field containing the actual event content.

Both classes offer a `to_json` method that returns the JSON representation of the record.
"""

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Literal, Dict, Any


# -------------------------------------------------------------------------------------------------
# Notification data structures
# -------------------------------------------------------------------------------------------------

@dataclass
class NotificationBody:
    """
    Structure of the notification payload.

    Attributes:
        message_id (str): The unique identifier of the message.
        chat_id (str): The unique identifier of the chat.
        sender_id (str): The unique identifier of the sender.
        message_text (str): The text content of the message.
        timestamp (str): The timestamp (in ISO format) when the notification was generated.
        (DEPR) is_read (bool): A flag indicating whether the notification has been read. Defaults to False.
    """
    message_id: str
    chat_id: str
    sender_id: str
    message_text: str
    timestamp: str

    def to_json(self) -> str:
        """
        Converts the NotificationBody instance to a JSON string.
        """
        return json.dumps(asdict(self))


@dataclass
class NotificationRecord:
    """
    Structure of a notification record stored in a notification table or cache.

    Attributes:
        type (Literal["message", "ack", "system", "other"]): The type of notification.
        body (NotificationBody): The detailed payload for the notification.
    """
    type: Literal["message", "ack", "system", "other"]
    body: NotificationBody

    def to_json(self) -> str:
        """
        Converts the NotificationRecord instance to a JSON string.
        """
        return json.dumps({
            "type": self.type,
            "body": asdict(self.body)
        })


# -------------------------------------------------------------------------------------------------
# Kafka event data structures
# -------------------------------------------------------------------------------------------------

@dataclass
class KafkaMessageEventBody:
    """
    Structure of the message event payload.

    Attributes:
        message_id (str): The unique identifier of the message.
        chat_id (str): The unique identifier of the chat.
        sender_id (str): The unique identifier of the sender.
        message_text (str): The text content of the message.
        timestamp (str): The timestamp (in ISO format) when the message was sent.
    """
    message_id: str
    chat_id: str
    sender_id: str
    message_text: str
    timestamp: str

    def to_json(self) -> str:
        """
        Converts the MessageEventBody instance to a JSON string.
        """
        return json.dumps(asdict(self))

@dataclass
class KafkaAckChatEventBody:
    """
    Structure of the ack chat event payload.

    Attributes:
        chat_id (str): The unique identifier of the chat.
        sender_id (str): The unique identifier of the sender.
        timestamp (str): The timestamp (in ISO format) when the ack chat event was generated.
    """
    chat_id: str
    sender_id: str
    timestamp: str

    def to_json(self) -> str:
        """
        Converts the AckChatEventBody instance to a JSON string.
        """
        return json.dumps(asdict(self))

@dataclass
class KafkaMessageEventRecord:

    event_type: Literal["message"]
    body: KafkaMessageEventBody

    def to_json(self) -> str:
        """
        Converts the KafkaMessageEventRecord instance to a JSON string.
        """
        return json.dumps({
            "event_type": self.event_type,
            "body": asdict(self.body)
        })


@dataclass
class KafkaAckChatEventRecord:

    event_type: Literal["ack_chat"]
    body: KafkaAckChatEventBody

    def to_json(self) -> str:
        """
        Converts the KafkaAckChatEventRecord instance to a JSON string.
        """
        return json.dumps({
            "event_type": self.event_type,
            "body": asdict(self.body)
        })


