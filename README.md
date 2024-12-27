# FedEx Supply Chain Data Pipeline

## ⚠️ Disclaimer
This project uses synthetic data and is for demonstration purposes only. No real FedEx data or proprietary information is included.

End-to-end AWS data pipeline processing FedEx shipping data to optimize delivery performance and reduce costs. This project demonstrates the implementation of a scalable ETL pipeline using AWS cloud services to analyze shipping patterns, costs, and operational efficiency.
## Architecture
<img width="685" alt="Screenshot 2024-12-27 at 4 54 58 PM" src="https://github.com/user-attachments/assets/65952af8-21d7-4efb-9cd7-061820c1885f" />

## Tech Stack
- Data Storage: Amazon S3.
- Data Processing: AWS Glue ETL.
- Data Visualization: Power BI.
- Infrastructure as Code: AWS CloudFormation.
- Programming Languages: Python, SQL.

## Features

- Automated data ingestion from multiple source reports.
- Data quality validation and error handling.
- Dimensional modeling for efficient querying.
- Cost analysis and optimization dashboards.
- Historical trend analysis.

## Getting Started

1. Clone this repository
2. Set up AWS credentials
3. Create required AWS resources using the CloudFormation template
4. Update configuration files
5. Run sample ETL job

## Prerequisites

- AWS Account with appropriate permissions
- Python 3.8+
- AWS CLI configured
- Basic understanding of ETL processes

## ETL Process

- Raw data ingestion to S3
- Data validation and cleaning
- Transform into a dimensional model
- Load into S3/ RedShift
- Generate analytics views

## 📈 Dashboard Examples

- Shipping Cost Analysis
- Surcharge Impact Assessment
- Account Performance Metrics
- Delivery Performance Tracking

## 🤝 Contributing
- Contributions are welcome! Please feel free to submit a Pull Request.

