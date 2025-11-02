"""
GCP Pub/Sub Client for pulling meeting messages
"""

import base64
import json
import logging
from typing import Optional, Dict
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class PubSubClient:
    """Client for interacting with GCP Pub/Sub"""
    
    def __init__(self, project_id: str, subscription_name: str):
        """
        Initialize Pub/Sub client
        
        Args:
            project_id: GCP project ID
            subscription_name: Pub/Sub subscription name
        """
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"
        
        # Initialize the subscriber client
        self.subscriber = pubsub_v1.SubscriberClient()
        
        logger.info(f"Initialized Pub/Sub client for subscription: {self.subscription_path}")
    
    def pull_one_message(self, max_messages: int = 1) -> Optional[Dict]:
        """
        Pull one message from the subscription with auto-ack
        
        Args:
            max_messages: Maximum number of messages to pull (default: 1)
            
        Returns:
            Decoded message data as dict, or None if no messages available
        """
        try:
            logger.info(f"Pulling message from subscription: {self.subscription_path}")
            
            # Pull messages synchronously
            response = self.subscriber.pull(
                request={
                    "subscription": self.subscription_path,
                    "max_messages": max_messages,
                }
            )
            
            if not response.received_messages:
                logger.info("No messages available in subscription")
                return None
            
            # Process the first message
            received_message = response.received_messages[0]
            message = received_message.message
            
            logger.info(f"Received message ID: {message.message_id}")
            logger.info(f"Published at: {message.publish_time}")
            
            # Decode the message data
            try:
                message_data = json.loads(message.data.decode('utf-8'))
                logger.info(f"Decoded message data: {message_data}")
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed to decode message data: {e}")
                # Still ack the message so it doesn't retry
                self._ack_message(received_message.ack_id)
                return None
            
            # Acknowledge the message
            self._ack_message(received_message.ack_id)
            
            return message_data
            
        except Exception as e:
            logger.exception(f"Error pulling message from Pub/Sub: {e}")
            return None
    
    def _ack_message(self, ack_id: str):
        """
        Acknowledge a message
        
        Args:
            ack_id: The ack ID of the message to acknowledge
        """
        try:
            self.subscriber.acknowledge(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [ack_id],
                }
            )
            logger.info(f"Acknowledged message with ack_id: {ack_id}")
        except Exception as e:
            logger.error(f"Failed to acknowledge message: {e}")
