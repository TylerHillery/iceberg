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

export REGION=[Enter your region]
echo "Region set to $REGION"

export BUCKET_NAME=[Enter your bucket name]
echo "Bucket name set to $BUCKET_NAME"

export NAMESPACE=[Enter your namespace]
echo "Namespace set to $NAMESPACE"

export TABLE_NAME=[Enter your table_name]
echo "Table name set to $TABLE_NAME"

export S3_TABLE_BUCKET_ARN=arn:aws:s3tables:$REGION:$AWS_ACCOUNT_ID:bucket/$BUCKET_NAME
echo "S3 table bucket ARN set to $S3_TABLE_BUCKET_ARN"
```

### Create Table Bucket

```sh
aws s3tables create-table-bucket \
    --region $REGION \
    --name $BUCKET_NAME
```

### Create Namespace

Think of a namespace as similar to a "schema" in Postgres.

```sh

aws s3tables create-namespace \
    --region $REGION \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN \
    --namespace "$NAMESPACE"

```

```sh
aws s3tables list-namespaces \
    --region $REGION \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN
```

### Create Table

```sh
aws s3tables create-table \
    --region $REGION \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN \
    --namespace $NAMESPACE \
    --name $TABLE_NAME \
    --cli-input-json file://mytabledefinition.json
```

# Teardown

### Delete Table

```sh
aws s3tables delete-table \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN \
    --namespace $NAMESPACE \
    --name $TABLE_NAME
echo "Table '$TABLE_NAME' has been deleted successfully"
```

### Delete Namespace

```sh
aws s3tables delete-namespace \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN \
    --namespace $NAMESPACE

echo "Namespace '$NAMESPACE' has been deleted successfully"
```

### Delete Table Bucket

```sh
aws s3tables delete-table-bucket \
    --region $REGION \
    --table-bucket-arn $S3_TABLE_BUCKET_ARN

echo "Table Bucket '$S3_TABLE_BUCKET_ARN' has been deleted successfully"
```
