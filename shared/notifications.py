#!/usr/bin/env python3
# Notifications Implementation

import os
import json
import logging
import smtplib
import requests
import time
from typing import Dict, Any, List, Optional
from email.mime.text import MIMEText

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_notifications(config: Dict[str, Any]) -> None:
    """
    Initialize notifications
    """
    # Check if notifications are enabled
    notifications_config = config.get('notifications', {})
    if not notifications_config.get('enabled', True):
        logger.info("Notifications are disabled")
        return
    
    logger.info("Initialized notifications")

def send_notification(notification_type: str, message: str, config: Dict[str, Any], severity: str = "medium", additional_recipients: List[str] = None) -> bool:
    """
    Send notification

    Args:
        notification_type: Type of notification (success, failure, circuit_open)
        message: Notification message
        config: Configuration dictionary
        severity: Severity level (low, medium, high)
        additional_recipients: Extra email addresses to include (additive to config recipients)
    """
    # Check if notifications are enabled
    notifications_config = config.get('notifications', {})
    if not notifications_config.get('enabled', True):
        logger.info("Notifications are disabled, skipping")
        return False
    
    # Check notification type settings
    if notification_type == 'success' and not notifications_config.get('on_success', False):
        logger.info("Success notifications are disabled, skipping")
        return False
    
    if notification_type == 'failure' and not notifications_config.get('on_failure', True):
        logger.info("Failure notifications are disabled, skipping")
        return False
    
    if notification_type == 'circuit_open' and not notifications_config.get('on_circuit_open', True):
        logger.info("Circuit open notifications are disabled, skipping")
        return False
    
    # Get source name
    source_name = config.get('source', {}).get('name', 'unknown')

    # Create consistent notification title format: "[Source] Pipeline Status"
    if notification_type == 'failure':
        title = f"[{source_name.upper()}] Pipeline Failed"
    elif notification_type == 'success':
        title = f"[{source_name.upper()}] Pipeline Success"
    elif notification_type == 'warning':
        title = f"[{source_name.upper()}] Pipeline Warning"
    else:
        title = f"[{source_name.upper()}] Pipeline {notification_type.title()}"

    # Add severity to title if specified
    if severity and severity != "medium":
        title = f"{title} - {severity.upper()}"
    
    # Send to each channel
    channels = notifications_config.get('channels', {})
    
    success = True
    
    # Send email notification
    email_config = channels.get('email', {})
    if email_config.get('enabled', True):
        email_success = send_email_notification(title, message, email_config, additional_recipients)
        success = success and email_success
    
    # Send Slack notification
    slack_config = channels.get('slack', {})
    if slack_config.get('enabled', True):
        slack_success = send_slack_notification(title, message, slack_config, severity)
        success = success and slack_success
    
    # Send SMS notification
    sms_config = channels.get('sms', {})
    if sms_config.get('enabled', False):
        sms_success = send_sms_notification(title, message, sms_config)
        success = success and sms_success
    
    return success

def send_email_notification(title: str, message: str, email_config: Dict[str, Any], additional_recipients: List[str] = None) -> bool:
    """
    Send email notification

    Args:
        title: Email subject
        message: Email body
        email_config: Email configuration dictionary
        additional_recipients: Extra email addresses to include (additive)
    """
    try:
        # Get email configuration
        smtp_host = email_config.get('smtp_host', os.environ.get('SMTP_HOST', 'smtp.gmail.com'))
        smtp_port = email_config.get('smtp_port', os.environ.get('SMTP_PORT', 587))
        smtp_user = email_config.get('smtp_user', os.environ.get('SMTP_USER'))
        smtp_password = email_config.get('smtp_password', os.environ.get('SMTP_PASSWORD'))
        from_email = email_config.get('from_email', os.environ.get('SMTP_FROM', 'pipeline@company.com'))

        # Get base recipients from config
        base_recipients = email_config.get('recipients', [])

        # Merge with additional recipients (if provided)
        recipients = base_recipients.copy()
        if additional_recipients:
            recipients.extend(additional_recipients)
            # Remove duplicates while preserving order
            recipients = list(dict.fromkeys(recipients))
        
        # Check if we have required configuration
        if not smtp_user or not smtp_password or not recipients:
            logger.warning("Missing email configuration, skipping email notification")
            return False
        
        # Create text-based message
        msg = MIMEText(message, 'plain')
        msg['From'] = from_email
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = title
        
        # Connect to SMTP server
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        
        # Send email
        server.send_message(msg)
        server.quit()

        logger.info(f"Sent email notification to {len(recipients)} recipient(s): {', '.join(recipients)}")
        return True
    except Exception as e:
        logger.error(f"Error sending email notification: {e}")
        return False

def send_slack_notification(title: str, message: str, slack_config: Dict[str, Any], severity: str = "medium") -> bool:
    """
    Send Slack notification
    """
    try:
        # Get Slack configuration
        webhook_url = slack_config.get('webhook_url', os.environ.get('SLACK_WEBHOOK_URL'))
        channel = slack_config.get('channel', '#data-alerts')
        
        # Check if we have required configuration
        if not webhook_url:
            logger.warning("Missing Slack webhook URL, skipping Slack notification")
            return False
        
        # Determine color based on severity
        color = "#3AA3E3"  # Default blue
        if severity == "critical":
            color = "#FF0000"  # Red
        elif severity == "high":
            color = "#FFA500"  # Orange
        elif severity == "low":
            color = "#36A64F"  # Green
        
        # Create payload
        payload = {
            "channel": channel,
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "ts": int(time.time())
                }
            ]
        }
        
        # Send notification
        response = requests.post(webhook_url, json=payload)
        
        if response.status_code != 200:
            logger.error(f"Error sending Slack notification: {response.text}")
            return False
        
        logger.info("Sent Slack notification")
        return True
    except Exception as e:
        logger.error(f"Error sending Slack notification: {e}")
        return False

def send_sms_notification(title: str, message: str, sms_config: Dict[str, Any]) -> bool:
    """
    Send SMS notification
    """
    try:
        # Get Twilio configuration
        account_sid = sms_config.get('twilio_account_sid', os.environ.get('TWILIO_ACCOUNT_SID'))
        auth_token = sms_config.get('twilio_auth_token', os.environ.get('TWILIO_AUTH_TOKEN'))
        from_number = sms_config.get('from_number', os.environ.get('TWILIO_PHONE_NUMBER'))
        
        # Get recipients
        recipients = sms_config.get('recipients', [])
        
        # Check if we have required configuration
        if not account_sid or not auth_token or not from_number or not recipients:
            logger.warning("Missing SMS configuration, skipping SMS notification")
            return False
        
        # Import Twilio client
        try:
            from twilio.rest import Client
        except ImportError:
            logger.warning("Twilio library not installed, skipping SMS notification")
            return False
        
        # Create client
        client = Client(account_sid, auth_token)
        
        # Create SMS message (truncate if too long)
        sms_message = f"{title}: {message}"
        if len(sms_message) > 1600:
            sms_message = sms_message[:1597] + "..."
        
        # Send to each recipient
        success = True
        for recipient in recipients:
            try:
                client.messages.create(
                    body=sms_message,
                    from_=from_number,
                    to=recipient
                )
            except Exception as e:
                logger.error(f"Error sending SMS to {recipient}: {e}")
                success = False
        
        logger.info(f"Sent SMS notification to {len(recipients)} recipients")
        return success
    except Exception as e:
        logger.error(f"Error sending SMS notification: {e}")
        return False