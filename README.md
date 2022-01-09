# aws_clients

API Clients package to make use of the AWS Python SDK to manage AWS Services.

## Setup

Requires python >= 3.7

Install with pip using this github repo with:

```sh
pip install --upgrade git+https://git@github.com/move13-camugo/aws_clients.git
```

## Use

Import the module as `camugo_aws_clients`

Example use:

```python
from camugo_aws_clients.clients import S3BucketManager
s3_manager = S3BucketManager(bucket_name="MyBucket", region="us-east-1")
s3_manager.list_objects()
```
