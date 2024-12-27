# src/utils/notifications.py
import boto3
import json
from datetime import datetime

class PipelineNotifier:
    def __init__(self, topic_arn):
        self.sns = boto3.client('sns')
        self.topic_arn = topic_arn

    def send_notification(self, subject, message, severity='INFO'):
        """
        Send SNS notification about pipeline events
        """
        notification = {
            'timestamp': datetime.now().isoformat(),
            'severity': severity,
            'message': message,
            'details': {}
        }

        try:
            self.sns.publish(
                TopicArn=self.topic_arn,
                Subject=f"FedEx Pipeline: {subject}",
                Message=json.dumps(notification, indent=2)
            )
        except Exception as e:
            print(f"Failed to send notification: {str(e)}")

    def alert_data_quality(self, validation_results):
        """
        Send notification about data quality issues
        """
        failed_validations = [k for k, v in validation_results.items() if not v]
        if failed_validations:
            self.send_notification(
                subject="Data Quality Alert",
                message=f"Failed validations: {', '.join(failed_validations)}",
                severity='ERROR'
            )

    def alert_pipeline_success(self, file_name, record_count):
        """
        Send notification about successful pipeline execution
        """
        self.send_notification(
            subject="Pipeline Success",
            message=f"Successfully processed {file_name} with {record_count} records"
        )
