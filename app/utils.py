import os
import gzip
import json
import boto3
import asyncio
import aiohttp
import urllib
import platform
from typing import Optional
from tqdm.auto import tqdm
from aiolimiter import AsyncLimiter
from aiohttp import ClientSession
from aiohttp_retry import RetryClient, ExponentialRetry

if platform.system().lower() == "windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

limiter = AsyncLimiter(max_rate=1, time_period=2)


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


async def download_modified(
        url: str,
        return_sub_id_from_url: bool = False,
        return_sub_id_from_dict: bool = False,
) -> Optional[dict]:
    if return_sub_id_from_dict:
        sub_id, url = list(url.keys())[0], list(url.values())[0]

    elif return_sub_id_from_url:
        sub_id = url.split("/")[-1]

    else:
        sub_id = None

    timeout = aiohttp.ClientTimeout(total=60 * 60)
    retry_client = RetryClient(raise_for_status=False, retry_options=ExponentialRetry(attempts=3), timeout=timeout)
    async with limiter:
        try:
            async with retry_client.get(url=url) as response:
                response_json = await response.json()
                response_json["submission_id"] = sub_id
            await retry_client.close()
        except aiohttp.client_exceptions.ContentTypeError as e:
            response_json = None

    return response_json


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx: min(ndx + n, l)]


def uri_validate(x):
    try:
        valid_result = urllib.parse.urlparse(x)
        assert all(
            [valid_result.scheme, valid_result.netloc]
        ), "Please validate `comment_ids_urls` passed as list."
    except:
        return False


async def request_proxy_ip_with_sessions(session: ClientSession):
    apikey = "G1K7leIQruMpapvpvQewPXLch3ArH_7Cle8dl3ev8ns"
    url = f"https://api.proxyorbit.com/v1/?token={apikey}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
        "Accept-Encoding": "*",
        "Connection": "keep-alive",
    }
    response = await session.get(url, headers=headers)
    response.raise_for_status()
    response_json = await response.json()
    return {"http": response_json["curl"], "https": response_json["curl"]}


async def fetch_all_proxies_async(count: int):
    async with ClientSession() as session:
        tasks = [
            asyncio.create_task(request_proxy_ip_with_sessions(session=session)) for _ in range(count)
        ]
        all_proxies = []
        for future in tqdm(
                asyncio.as_completed(tasks), total=count, desc="Downloading all proxies..."
        ):
            result = await future
            all_proxies.append(result)

    return all_proxies


async def request_proxy_ip():
    apikey = "G1K7leIQruMpapvpvQewPXLch3ArH_7Cle8dl3ev8ns"
    url = (
        f"https://api.proxyorbit.com/v1/?token={apikey}&protocol=http&ssl=false&get=true"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
        "Accept-Encoding": "*",
        "Connection": "keep-alive",
    }
    async with aiohttp.request(method="GET", headers=headers, url=url) as response:
        response.raise_for_status()
        response_json = await response.json()

    return {"http": response_json["curl"], "https": response_json["curl"]}


def upload_json_gz_to_s3(key: str, obj: list):
    s3_client = boto3.client("s3")
    bytes_data = io.BytesIO()
    with gzip.GzipFile(fileobj=bytes_data, mode="wb") as bf:
        with io.TextIOWrapper(bf, encoding="utf-8") as wrapper:
            wrapper.write(json.dumps(obj, ensure_ascii=False, default=None))
    bytes_data.seek(0)
    s3_client.upload_fileobj(
        bytes_data,
        "polygonio-dumps",
        key,
    )


def download_json_gz_from_s3(key):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket="polygonio-dumps", Key=key)
    content = response["Body"].read()
    with gzip.GzipFile(fileobj=io.BytesIO(content), mode="rb") as fh:
        return json.load(fh)
