import logging
import os
import boto3
import logging


def get_s3_client(logger):
    aws_access_key, aws_secret_key = os.getenv("AWS_ACCESS_KEY_ID"), os.getenv(
        "AWS_SECRET_ACCESS_KEY"
    )
    if aws_access_key and aws_secret_key:
        s3 = boto3.client(
            "s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
        )
    else:
        logger.error("AWS Environment vars not set.")
        raise (EnvironmentError, "AWS Environment vars not set.")
    return s3


def s3_key_exists(client: boto3.client, bucket: str, key: str):
    response = client.list_objects(Bucket=bucket)
    if response:
        for obj in response["Contents"]:
            if key == obj["Key"]:
                return True
    return False


def write_to_s3(key: str, results: list = None, errors: list = None):
    if results:
        json_key = "all_results"
    elif errors:
        json_key = "all_submission_errors"
    else:
        json_key = ""

    with io.StringIO() as buffer:
        json.dump({json_key: results}, buffer)
        with io.BytesIO(buffer.getvalue().encode()) as buffer2:
            s3_client.upload_fileobj(
                buffer2,
                "polygonio-dumps",
                key,
            )


# for all downloads, just a small func
async def download(url, session) -> dict:
    async with limiter:
        try:
            response = await session.request(url=url, method="GET")
            response.raise_for_status()
            response_json = await response.json()
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            response_json = json.dumps({"error_url": url})
        except Exception as err:
            response_json = None
            print(f"An error occurred: {err}")
    return response_json


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]
