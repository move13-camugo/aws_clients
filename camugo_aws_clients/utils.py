import boto3
import json
import os
import sys
import threading


class ProgressPercentage(object):
    """
    Progress monitor class implementation
    that can be called to monitor the bytes transfer
    of an upload_file or upload_fileobj operation
    using the Callback parameter [1].

    References
    ----------
        - [1] https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html#the-callback-parameter
    """

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()

def get_session_from_json_credentials(
    file_path: str, organization: str, profile: str = "default", region_name: str = None
) -> boto3.Session:
    """
    Gets the session from boto3 using the credentials stored in a JSON file

    Parameters
    ----------
        - `file_path` <str>: Machine path to JSON file with credentials.
        - `organization` <str>: Name of the organization in which to get the credentials. 
        - `profile` <str>: Name of the user profile of the organization to get the credentials from. Defaults to 'default'.
        - `region_name` <str>: Default region when creating new connections [1].

    The JSON file must be a dict in which the keys are the name of the organizations. eg:

    ```json
    {   
        "personal":{ // <- organization
            "credentials": {
                "default":{ // <- profile
                    "key_id":"ABCDEFGHIJKLMNOPQ",
                    "access_key": "qwerty123qwerty123/qwerty1234"
                }
            }
        },
        "MyCoolOrganization":{
            "credentials":{
                "s3-only":{
                    "key_id": ... ,
                    "access_key": ...,
                }
            }, 
            ...
        },
        ...
    }
    ```

    Returns
    -------
        A `boto3.Session` object that can be used when instantiating an aws client.

    Raises
    ------
        - `KeyError`: When the json file does not have the correct format.

    References
    ----------
        - [1] https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
    """
    with open(file_path, "r") as f:
        credentials = json.load(f)
    try:
        session = boto3.Session(
            aws_access_key_id=credentials[organization]["credentials"][profile][
                "key_id"
            ],
            aws_secret_access_key=credentials[organization]["credentials"][profile][
                "access_key"
            ],
            region_name=region_name,
        )
    except KeyError as e:
        raise KeyError(f"Malformed json credentials file.") from e

    return session
