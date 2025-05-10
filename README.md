# 🛠️ Raw Transaction Handler - AWS Lambda Data Processing Pipeline

[![codecov](https://codecov.io/gh/Djirlic/raw-transactions-handler/graph/badge.svg?token=6SCLIH5NTD)](https://codecov.io/gh/Djirlic/raw-transactions-handler)
![CI](https://github.com/Djirlic/raw-transactions-handler/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.13+-blue)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen)

A serverless data ingestion and transformation pipeline using AWS Lambda, designed to validate, transform, and route raw financial transaction data to a refined data bucket. The pipeline also maintains structured logs for successful and failed processing attempts.

## 💡 Motivation

This project automates the validation and transformation of raw transaction data using a serverless architecture. It is specifically tailored for this [dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection) with Credit Card Fraud Detection data, ensuring data integrity through schema validation, field checks, and error handling. It is part of a larger data processing workflow that organizes data for downstream analytics and reporting.

## 🚀 Features

- Data Validation:
  - Ensures schema consistency (e.g., expected columns, data types).
  - Validates specific fields such as ZIP codes and fraud indicators.
- Data Transformation:
  - Converts valid CSV data to Parquet format for optimized storage and processing.
- Data Routing:
  - Moves valid data to a refined S3 bucket.
  - Routes invalid data to a quarantine folder for further inspection.
- Logging:
  - Logs successful data ingestions in refinement-log.json.
  - Logs data validation failures in quarantine-log.json.
- CI/CD:
  - Fully automated CI/CD steps to check code formatting, linting, import sorting, and deploy both the Lambda function as well as the AWS Layer.

## ✅ Prerequisites

- **AWS CLI** configured with a profile that has access to the S3 buckets.
- Two S3 buckets:
  - Raw data bucket (e.g., raw-credit-card-transactions).
  - Refined data bucket (e.g., refined-credit-card-transactions).
- AWS Lambda function configured with appropriate IAM roles and policies:
  - Read access to the raw bucket.
  - Write access to the refined bucket and quarantine folder.
  - Permission to update the log files.
- Environment variable with the name of the refined bucket.

## ⚙️ Implementation Details

The Lambda function is triggered upon new file uploads to the raw bucket. It executes the following steps:

1. Download the CSV file.
2. Validate the data structure:
  - Ensure all expected columns are present.
  - Verify ZIP code format.
  - Check that is_fraud is only 0 or 1.
3. Transform valid data to Parquet format.
4. Upload Parquet file to the refined bucket.
5. Log the processing outcome:
  - Update refinement-log.json for successful ingestions.
  - Update quarantine-log.json for invalid data.

On error the CSV will be uploaded to a quarantine folder in the refined bucket for investigation and re-run.

### 🧪 Testing

Run tests using:

```bash
make test
```

Tests are structured to verify:

- Schema validation.
- Handling of missing or invalid columns.
- Data transformation to Parquet.
- S3 interactions (download, upload, logging).

## 🛠️ Future Improvements

- Implement retry logic for transient S3 failures.
- Expand field-level validation.

## 📢 Feedback & Contributions

Have suggestions or ideas? Feel free to open an issue.
