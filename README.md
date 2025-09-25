# aws-stock-market-analytics-pipeline
End-to-end, near real-time analytics pipeline on AWS for streaming stock data. Uses Amazon Kinesis, AWS Lambda, Amazon S3, Amazon DynamoDB, AWS Glue Data Catalog, and Amazon Athena to ingest, process, store, and query ticks, with SMA-5/20 trend detection and buy/sell alerts via Amazon SNS. IaC with AWS SAM and least-privilege IAM.
