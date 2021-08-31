# aws_clients

API Clients package to make use of the AWS Python SDK to manage AWS Services.

## Setup

Requires python >= 3.7

Install with pip by setting an [SSH deploy key](https://docs.github.com/en/developers/overview/managing-deploy-keys) as an environment variable:

```sh
export GIT_SSH_COMMAND=ssh -i ~/.ssh/<your-deploy-key>
```

Then run:

```sh
pip install git+ssh://git@github.com/move13-camugo/aws_clients.git
```

If you prefer to use a [personal access token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token) you must set the environment variable `GIT_PERSONAL_ACCESS_TOKEN` to the value of the token and then run:

```sh
pip install git+https://${GIT_PERSONAL_ACCESS_TOKEN}@github.com/move13-camugo/aws_clients.git
```

You must configure your aws credentials so the SDK can access them, the easiest way to do this is to set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables. Check the [Boto3 Configuration Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#guide-configuration) for more details.

## Use

Import the module as `aws_clients`

Example use:

```python
from aws_clients.clients import S3BucketManager
s3_manager = S3BucketManager(bucket_name="MyBucket", region="us-east-1")
s3_manager.list_objects()
```
