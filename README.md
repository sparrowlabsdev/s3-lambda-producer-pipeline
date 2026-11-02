# Data lake reference architectures

Two small, production-shaped reference architectures for an S3 data lake, each deployable with Terraform:

| # | Architecture | Direction | Where |
|---|---|---|---|
| 1 | **S3 → Lambda → Glue producer pipeline** | produce: CSV lands in S3, Glue crawls and converts to parquet | this directory (below) |
| 2 | **Delta Sharing → S3 Delta Lake mirror** | consume: tables shared over the open Delta Sharing protocol are mirrored as plain Delta tables on S3 (ECS Fargate, no Spark, Databricks-ready) | [`delta-sharing-mirror/`](delta-sharing-mirror/README.md) |

---

# 1. s3-lambda-producer-pipeline


The Terraform template deploys a data producer using a series of Lambda functions, S3 buckets, EventBridge rules, and IAM resources required to run the application. 

A Lambda function consumes `ObjectCreated` events from an Amazon S3 bucket to start a glue crawler generating a data catalog.  An Eventbridge rule is used to trigger a Lambda to start the transform job which converts a CSV file into snappy parquet format file and saves to an output bucket.

## Notes

1.  This blueprint will create a vpc as part of the deployment unless `vpc_id` is passed as a variable to terraform commands.

## Deployment Instructions

1. From the command line, initialize terraform to download and install the providers defined in the configuration:
    ```
    terraform init
    ```
1. From the command line, apply the configuration in the main.tf file:
    ```
    terraform apply
    ```
1. Note the outputs from the deployment process. These contain the resource names and/or ARNs which are used for testing.

## How it works

When we upload an object to S3, this will trigger the Lambda function which will start the data pipeline.

## Testing

After deployment, create a folder named `patients` in the S3 input bucket and upload the patients.csv file from the `data/` folder. Go to the CloudWatch Logs for the deployed Lambda function. You will see the event is logged out containing the Object data.  Go to the AWS Glue catalog to see the crawler status and tables created.  You can check the output bucket after approximately 5 minutes.


