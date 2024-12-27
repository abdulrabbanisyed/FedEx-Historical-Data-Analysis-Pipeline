# src/utils/error_handler.py
import logging
import traceback
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional

class PipelineErrorHandler:
    def __init__(self, sns_topic_arn: str):
        self.logger = logging.getLogger(__name__)
        self.sns = boto3.client('sns')
        self.sns_topic_arn = sns_topic_arn
        self.setup_logging()

    def setup_logging(self):
        """Configure logging with detailed formatting"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def handle_s3_errors(self, error: ClientError, file_path: str) -> Dict[str, Any]:
        """Handle S3-specific errors"""
        error_code = error.response['Error']['Code']
        error_mapping = {
            'NoSuchBucket': {
                'action': 'Check if bucket exists and you have proper permissions',
                'severity': 'HIGH',
                'retry': False
            },
            'NoSuchKey': {
                'action': 'Verify file path and name',
                'severity': 'MEDIUM',
                'retry': True
            },
            'AccessDenied': {
                'action': 'Check IAM permissions and bucket policy',
                'severity': 'HIGH',
                'retry': False
            }
        }

        error_info = error_mapping.get(error_code, {
            'action': 'General S3 error, check AWS logs',
            'severity': 'MEDIUM',
            'retry': True
        })

        self._log_and_notify(
            error=error,
            context=f"S3 operation failed for {file_path}",
            error_info=error_info
        )
        return error_info

    def handle_glue_errors(self, error: Exception, job_name: str) -> Dict[str, Any]:
        """Handle Glue job-specific errors"""
        error_mapping = {
            'ConcurrentRunsExceededException': {
                'action': 'Wait for other jobs to complete or increase concurrent run limit',
                'severity': 'MEDIUM',
                'retry': True
            },
            'ResourceNotFoundException': {
                'action': 'Verify Glue job name and permissions',
                'severity': 'HIGH',
                'retry': False
            },
            'ValidationException': {
                'action': 'Check job parameters and configuration',
                'severity': 'HIGH',
                'retry': False
            }
        }

        error_info = error_mapping.get(error.__class__.__name__, {
            'action': 'General Glue error, check job logs',
            'severity': 'MEDIUM',
            'retry': True
        })

        self._log_and_notify(
            error=error,
            context=f"Glue job {job_name} failed",
            error_info=error_info
        )
        return error_info

    def handle_data_validation_error(self, error_details: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data validation errors"""
        error_info = {
            'missing_values': {
                'action': 'Check source data completeness',
                'severity': 'MEDIUM',
                'retry': False
            },
            'invalid_format': {
                'action': 'Verify data format matches expected schema',
                'severity': 'HIGH',
                'retry': False
            },
            'business_rule_violation': {
                'action': 'Review business logic and data rules',
                'severity': 'HIGH',
                'retry': False
            }
        }

        error_type = error_details.get('type', 'unknown_validation_error')
        error_response = error_info.get(error_type, {
            'action': 'General validation error, check validation logs',
            'severity': 'MEDIUM',
            'retry': False
        })

        self._log_and_notify(
            error=Exception(error_details.get('message', 'Validation Error')),
            context="Data validation failed",
            error_info=error_response
        )
        return error_response

    def _log_and_notify(self, error: Exception, context: str, error_info: Dict[str, Any]):
        """Log error and send SNS notification"""
        error_message = {
            'timestamp': datetime.now().isoformat(),
            'context': context,
            'error_type': error.__class__.__name__,
            'error_message': str(error),
            'stack_trace': traceback.format_exc(),
            'severity': error_info['severity'],
            'recommended_action': error_info['action'],
            'retry_recommended': error_info['retry']
        }

        # Log error
        self.logger.error(f"Pipeline Error: {error_message}")

        # Send SNS notification
        try:
            self.sns.publish(
                TopicArn=self.sns_topic_arn,
                Subject=f"Pipeline Error: {error_info['severity']} Severity",
                Message=str(error_message)
            )
        except Exception as e:
            self.logger.error(f"Failed to send error notification: {str(e)}")
