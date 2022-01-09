import os
import boto3
import json

class LambdaClient:

	def __init__(self, function_name:str, region:str=None):
		"""
		Initiates a lambda client using the name of the function given as input.
		"""
		self.region = region if region != None else boto3.session.Session().region_name
		self.client = boto3.client('lambda', region_name=self.region)
		self.function_name = function_name

	@staticmethod
	def list_all_functions(region_name:str) -> list:
		"""
		Returns a list of all the lambda functions in the given region.
		"""
		functions = boto3.client('lambda', region_name=region_name).list_functions()['Functions']
		return list(functions)

	def invoke(self, event:dict, run_async:bool=False) -> dict:
		"""
		Invokes the lambda function using the given parameter.

		Parameters
		----------
		`event`: <dict>
			The JSON that you want to provide to your Lambda function as input
		`run_async`: <bool>:
			Whether to wait for the function response or just send the event asynchronously.

		Returns
		-------
		`dict`
			Response of invoking the Lambda function.
		"""
		response = self.client.invoke(
			FunctionName=self.function_name,
			InvocationType='RequestResponse' if not run_async else 'Event',
			LogType='Tail',
			Payload=json.dumps(event)
		)
		payload = response['Payload'].read().decode("utf-8") if response.get('Payload') else None
		response['Payload'] = json.loads(payload) if payload else None
		return response

if __name__ == "__main__":

	region = 'us-east-2'
	cl = LambdaClient('test-func', region)
	payload = {
	  "Records": [
		{
		  "eventVersion": "2.0",
		  "eventSource": "aws:s3",
		  "awsRegion": "us-east-1",
		  "eventTime": "1970-01-01T00:00:00.000Z",
		  "eventName": "ObjectCreated:Put",
		  "userIdentity": {
			"principalId": "EXAMPLE"
		  },
		  "requestParameters": {
			"sourceIPAddress": "127.0.0.1"
		  },
		  "responseElements": {
			"x-amz-request-id": "EXAMPLE123456789",
			"x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
		  },
		  "s3": {
			"s3SchemaVersion": "1.0",
			"configurationId": "testConfigRule",
			"bucket": {
			  "name": "testvideosm",
			  "ownerIdentity": {
				"principalId": "EXAMPLE"
			  },
			  "arn": "arn:aws:s3:::testvideosm"
			},
			"object": {
			  "key": "document_test_1.txt",
			  "size": 1024,
			  "eTag": "0123456789abcdef0123456789abcdef",
			  "sequencer": "0A1B2C3D4E5F678901"
			}
		  }
		}
	  ]
	}

	# print(LambdaClient.list_all_functions(region))
	response = cl.invoke(payload, run_async=True)
	print(json.dumps(response, indent=4))	
	print("success = ", response["StatusCode"] in range(200, 300)) # 2xx Success
