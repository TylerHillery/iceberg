## Instructions

**Resource**

- [Tutorial: Getting started with S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-getting-started.html)

The below instructions show how to use the AWS CLI to create S3 Table Bucket resources.

If you prefer to use PyIceberg, follow the PyIceberg example in [setup.py](./setup.py).

You can make this README interactive by opening it with [RUNME.dev](https://runme.dev/).

### Setup Configuration

```sh {"promptEnv":"auto"}
export AWS_ACCOUNT_ID=[Enter your aws account id]
echo "AWS account id set to $AWS_ACCOUNT_ID"

export AWS_REGION=[Enter your region]
echo "Region set to $AWS_REGION"

export S3_TABLE_BUCKET_NAME=[Enter your bucket name]
echo "Bucket name set to $S3_TABLE_BUCKET_NAME"

export S3_TABLE_NAMESPACE=[Enter your namespace]
echo "Namespace set to $S3_TABLE_NAMESPACE"

export S3_TABLE_NAME=[Enter your table_name]
echo "Table name set to $S3_TABLE_NAME"

export S3_TABLE_BUCKET_ARN=arn:aws:s3tables:$AWS_REGION:$AWS_ACCOUNT_ID:bucket/$S3_TABLE_BUCKET_NAME
echo "S3 table bucket ARN set to $S3_TABLE_BUCKET_ARN"
```

### Create Table Bucket

```sh
aws s3tables create-table-bucket \
    --region $AWS_REGION \
    --name $S3_TABLE_BUCKET_NAME
```

### Create Namespace

Think of a namespace as similar to a "schema" in Postgres.

```sh
aws s3tables create-namespace \
    --region $AWS_REGION \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN \
    --namespace "$S3_TABLE_NAMESPACE"

```

```sh
aws s3tables list-namespaces \
    --region $AWS_REGION \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN
```

### Create Table

```sh
aws s3tables create-table \
    --region $AWS_REGION \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN \
    --namespace $S3_TABLE_NAMESPACE \
    --name $S3_TABLE_NAME \
    --cli-input-json file://mytabledefinition.json
```

# Teardown

### Delete Table

```sh
aws s3tables delete-table \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN \
    --namespace $S3_TABLE_NAMESPACE \
    --name $S3_TABLE_NAME
echo "Table '$S3_TABLE_NAME' has been deleted successfully"
```

### Delete Namespace

```sh
aws s3tables delete-namespace \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN \
    --namespace $S3_TABLE_NAMESPACE

echo "Namespace '$S3_TABLE_NAMESPACE' has been deleted successfully"
```

### Delete Table Bucket

```sh
aws s3tables delete-table-bucket \
    --region $AWS_REGION \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN

echo "Table Bucket '$S3_TABLE_BUCKET_ARN' has been deleted successfully"
```
