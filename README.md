# Deezer-End-2-End-ETL-Pipeline-Project
Built a serverless ETL pipeline using AWS services including Lambda, S3, CloudWatch, Glue Catalog with Crawler, and Athena for data processing, storage, monitoring, and querying.

# Serverless ETL Pipeline with AWS

## 📌 Project Overview

This project implements a **serverless ETL (Extract, Transform, Load) pipeline** using AWS services. The pipeline automates the ingestion, processing, cataloging, and querying of data using event-driven architecture and fully managed AWS tools.

---

## 🛠️ AWS Services Used

- **AWS Lambda** – For processing and transforming data
- **Amazon S3** – For storing raw and processed data
- **Amazon CloudWatch** – For logging and monitoring Lambda functions
- **AWS Glue Catalog & Crawler** – For schema discovery and metadata management
- **Amazon Athena** – For querying structured data stored in S3

---

## 🔄 ETL Workflow

1. **Data Ingestion**  
   - Files are uploaded to an S3 bucket by executing the deezer_extract_function_lambda.py Lambda function.

2. **Trigger & Processing**  
   - The S3 upload triggers an AWS Lambda function. Trigger is created using Cloudwatch
   - Lambda reads, cleans, or transforms the data.

3. **Data Storage**  
   - Processed data is stored back in another S3 location.

4. **Cataloging**  
   - AWS Glue Crawler runs on the processed data.
   - It updates the Glue Data Catalog with the latest schema.

5. **Querying**  
   - Athena queries are run on the cataloged data for analysis.

---

## 🧪 Testing & Monitoring

- Use the Lambda test feature or trigger events by uploading files to S3.
- Monitor function execution and errors using **CloudWatch Logs**.
- Use **Athena** to verify data integrity and query results.


