import asyncio
import datetime
import io
import json
import logging
import os
import platform
from urllib.parse import urlparse, urlencode

from aiohttp import ClientSession
from aiolimiter import AsyncLimiter
from requests.exceptions import HTTPError
from tqdm.auto import tqdm

from utils import s3_key_exists, get_s3_client, write_to_s3, download

# setup logging here
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[logging.FileHandler("subs_logs.log"), logging.StreamHandler()],
)

# setup async windows settings here settings here
system = platform.system().lower()
if system == "windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# set up limiter
limiter = AsyncLimiter(max_rate=1, time_period=2)


# For all submissions
def get_submission_search_urls(sdate: str, edate: str) -> list:
    base_url = "https://api.pushshift.io/reddit/search/submission/?"
    # submission_status_df = gather_wsb.get_submission_status_mat_view(local=True)

    sdate, edate = datetime.datetime.strptime(sdate, "%Y-%m-%d"), datetime.datetime.strptime(edate, "%Y-%m-%d")
    date_range = [edate - datetime.timedelta(days=x) for x in range((edate - sdate).days)]

    # For missing_start_end_dates
    missing_dates = list(set(date_range))
    missing_dates_pairs = [
        (
            str(int((x - datetime.timedelta(days=1)).timestamp())),
            str(int((x + datetime.timedelta(days=1)).timestamp())),
        )
        for x in missing_dates
    ]

    # Make all params
    all_urls = []
    for start, end in missing_dates_pairs:
        parsed = urlparse(base_url)
        params = {
            "subreddit": "wallstreetbets",
            "after": start,
            "before": end,
            "sort_type": "num_comments",
            "sort": "desc",
            "limit": "1000",
        }
        params = urlencode(params)
        parsed = parsed._replace(query=params)
        all_urls.append(parsed.geturl())

    return all_urls


async def extract_submissions(
        sdate: str, edate: str, urls=None
) -> [list, list]:
    if urls is None:
        urls = get_submission_search_urls(sdate=sdate, edate=edate)

    cols = ["created_utc", "id", "author", "url", "title", "selftext", "stickied"]
    not_errors, errors = [], []
    async with ClientSession() as session:
        tasks = [asyncio.create_task(download(url, session)) for url in urls]

        for future in tqdm(asyncio.as_completed(tasks), total=len(urls)):
            res = await future

            if "error_url" in res.keys():
                errors.append(res["error_url"])

            elif "data" in res.keys():
                res = res["data"]

                for result in res:
                    r = {
                        col: (result[col] if col in result.keys() else None)
                        for col in cols
                    }
                    not_errors.append(r)
    return not_errors, errors


if __name__ == "__main__":
    logging.info("Fetching s3 client...")
    s3_client = get_s3_client(logger=logging)
    logging.info("Fetched!...")

    # get start and end date from sys env variables
    logging.info("Fetch start and end dates...")
    start_date, end_date = os.getenv("SDATE"), os.getenv("EDATE")

    logging.info("Fetched!")
    if start_date and end_date:
        logging.info("Extract submissions...")
        all_results, error_urls = asyncio.run(
            extract_submissions(sdate=start_date, edate=end_date)
        )
        logging.info("Extracted!")
    else:
        logging.error("ENV vars SDate and EDate not set.")
        raise (EnvironmentError, "ENV vars SDate and EDate not set.")

    # check if a file like "error urls" exists in s3, else just look for start/end date env variables
    logging.info("Checking if error urls for the same dates exist...")
    submission_errors_urls_keys = f"submission_error_urls_{start_date}_{end_date}.json"
    if s3_key_exists(
            client=s3_client,
            bucket="polygonio-dumps",
            key=submission_errors_urls_keys,
    ):
        logging.info(f"{submission_errors_urls_keys} exists and downloading!")
        jsonBuffer = io.StringIO()
        s3_client.download_file(
            "polygonio-dumps",
            submission_errors_urls_keys,
            jsonBuffer,
        )
        submission_error_urls = json.load(jsonBuffer)["all_submission_errors"]

        logging.info("Extract submissions for those error urls...")
        results_1, error_urls_1 = asyncio.run(extract_submissions(urls=submission_error_urls))
        logging.info("Extracted error urls!")

        all_results += results_1
        error_urls += error_urls_1

    # write both error urls and all_results to s3
    submission_results_key = f"submission_results_{start_date}_{end_date}.json"
    logging.info(f"Uploading all results to s3://polygonio-dumps/{submission_results_key}...")
    write_to_s3(key=submission_results_key, results=all_results)
    logging.info("Uploaded!")

    logging.info(f"Uploading all results to s3://polygonio-dumps/{submission_errors_urls_keys}")
    write_to_s3(key=submission_errors_urls_keys, errors=error_urls)
    logging.info("Uploaded!")
