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
            logger.debug(f"Max messages: {max_messages}")
            
            # First, verify the subscription exists and get its details
            try:
                subscription = self.subscriber.get_subscription(
                    request={"subscription": self.subscription_path}
                )
                logger.info(f"Subscription exists: {subscription.name}")
                logger.info(f"Topic: {subscription.topic}")
                logger.debug(f"Ack deadline: {subscription.ack_deadline_seconds}s")
            except Exception as sub_error:
                logger.error(f"Failed to get subscription details: {sub_error}")
                logger.error(f"This may indicate a permissions issue or invalid subscription name")
                return None
            
            # Pull messages synchronously
            logger.debug("Calling subscriber.pull()...")
            response = self.subscriber.pull(
                request={
                    "subscription": self.subscription_path,
                    "max_messages": max_messages,
                },
                timeout=30.0  # Add explicit timeout
            )
            
            logger.debug(f"Pull response received. Message count: {len(response.received_messages)}")
            
            if not response.received_messages:
                logger.info("No messages available in subscription")
                logger.debug("This could mean: 1) No messages in queue, 2) All messages being processed by other consumers, 3) Messages filtered by subscription filter")
                return None
            
            # Process the first message
            received_message = response.received_messages[0]
            message = received_message.message
            
            logger.info(f"Received message ID: {message.message_id}")
            logger.info(f"Published at: {message.publish_time}")
            logger.debug(f"Message attributes: {dict(message.attributes)}")
            logger.debug(f"Message data (raw bytes length): {len(message.data)}")
            
            # Decode the message data
            try:
                decoded_data = message.data.decode('utf-8')
                logger.debug(f"Decoded data (first 500 chars): {decoded_data[:500]}")
                
                message_data = json.loads(decoded_data)
                logger.info(f"Successfully decoded message data")
                logger.info(f"Message keys: {list(message_data.keys())}")
                logger.info(f"Full message data: {json.dumps(message_data, indent=2)}")
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed to decode message data: {e}")
                logger.error(f"Raw message data (first 1000 bytes): {message.data[:1000]}")
                # Still ack the message so it doesn't retry
                self._ack_message(received_message.ack_id)
                return None
            
            # Acknowledge the message
            self._ack_message(received_message.ack_id)
            
            return message_data
            
        except Exception as e:
            logger.exception(f"Error pulling message from Pub/Sub: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
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
    
    def check_subscription_status(self):
        """
        Check subscription status and configuration for debugging
        
        Returns:
            Dict with subscription details or None if error
        """
        try:
            logger.info("=" * 60)
            logger.info("SUBSCRIPTION DIAGNOSTIC CHECK")
            logger.info("=" * 60)
            
            # Get subscription details
            subscription = self.subscriber.get_subscription(
                request={"subscription": self.subscription_path}
            )
            
            logger.info(f"✓ Subscription Name: {subscription.name}")
            logger.info(f"✓ Topic: {subscription.topic}")
            logger.info(f"✓ Ack Deadline: {subscription.ack_deadline_seconds}s")
            logger.info(f"✓ Message Retention: {subscription.message_retention_duration}")
            logger.info(f"✓ Retain Acked Messages: {subscription.retain_acked_messages}")
            
            if subscription.filter:
                logger.info(f"✓ Filter: {subscription.filter}")
            else:
                logger.info(f"✓ Filter: None (all messages)")
            
            if subscription.dead_letter_policy:
                logger.info(f"✓ Dead Letter Topic: {subscription.dead_letter_policy.dead_letter_topic}")
            
            # Try to get approximate message count (this requires additional API call)
            try:
                from google.cloud.pubsub_v1.services.subscriber import SubscriberClient
                # Note: There's no direct API to get message count, but we can check if pull returns anything
                logger.info("✓ Attempting to peek for messages (non-consuming)...")
                
                # Do a pull with return_immediately (if available)
                test_response = self.subscriber.pull(
                    request={
                        "subscription": self.subscription_path,
                        "max_messages": 1,
                    },
                    timeout=5.0
                )
                
                if test_response.received_messages:
                    logger.info(f"✓ Messages available: YES (at least 1 message found)")
                    logger.info(f"  Message ID: {test_response.received_messages[0].message.message_id}")
                    logger.info(f"  Published: {test_response.received_messages[0].message.publish_time}")
                    
                    # Don't ack - let it be redelivered
                    logger.warning("  NOTE: Message NOT acknowledged - will be redelivered for processing")
                else:
                    logger.info(f"✓ Messages available: NO")
                    
            except Exception as peek_error:
                logger.warning(f"Could not peek for messages: {peek_error}")
            
            logger.info("=" * 60)
            
            return {
                "name": subscription.name,
                "topic": subscription.topic,
                "ack_deadline": subscription.ack_deadline_seconds,
            }
            
        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"✗ SUBSCRIPTION CHECK FAILED: {e}")
            logger.error(f"✗ Error type: {type(e).__name__}")
            logger.error("=" * 60)
            logger.error("\nPossible causes:")
            logger.error("  1. Subscription does not exist")
            logger.error("  2. Incorrect subscription path format")
            logger.error("  3. Missing IAM permissions (need 'pubsub.subscriptions.get')")
            logger.error("  4. Invalid GCP credentials")
            logger.error("  5. Wrong project ID")
            logger.error("=" * 60)
            return None
