Serverless-Data-Pipeline

fully automated serverless data processing and analytics pipeline built using AWS services.
This project demonstrates how raw order data stored in DynamoDB is automatically crawled, transformed, cleaned, stored, and queried using Glue, S3, and Athena — without managing any servers.


Project Highlights

100% Serverless (no EC2 / no manual servers)

DynamoDB → NoSQL data storage

Glue Crawler → Auto-schema detection

Glue ETL Job (PySpark) → Transform + filter data

S3 Data Lake → Store transformed Parquet files

Athena SQL Queries → Serverless analytics

End-to-end pipeline automation


Architecture Diagram  :  



![serverless-data-pipelinearchitecture-diagram png](https://github.com/user-attachments/assets/af6845cb-ab0f-4697-b3db-9f754f342f63)
